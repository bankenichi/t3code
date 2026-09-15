import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { orchestrationProtocolCompatibilityError } from "./compatibility.ts";

import * as ConnectionResolver from "./resolver.ts";
import * as ConnectionDriver from "./driver.ts";
import * as EnvironmentRegistry from "./registry.ts";
import * as ConnectionOnboarding from "./onboarding.ts";
import * as PlatformConnectionSource from "../platform/source.ts";
import * as RelayEnvironmentDiscovery from "../relay/discovery.ts";
import * as RemoteEnvironmentAuthorization from "../authorization/service.ts";
import * as RpcSession from "../rpc/session.ts";

export function layerWithOptions(options: RpcSession.RpcSessionOptions) {
  const driverLayer = ConnectionDriver.layer.pipe(
    Layer.provide(Layer.mergeAll(ConnectionResolver.layer, RpcSession.layerWithOptions(options))),
  );
  const registryLayer = EnvironmentRegistry.layer.pipe(Layer.provide(driverLayer));
  const onboardingLayer = ConnectionOnboarding.layer.pipe(Layer.provide(registryLayer));
  const connectionServicesLayer = Layer.mergeAll(
    registryLayer,
    RelayEnvironmentDiscovery.layer,
    onboardingLayer,
  );
  const connectionStartupLayer = Layer.effectDiscard(
    Effect.gen(function* () {
      const registry = yield* EnvironmentRegistry.EnvironmentRegistry;
      const platformSource = yield* PlatformConnectionSource.PlatformConnectionSource;
      const discovery = yield* RelayEnvironmentDiscovery.RelayEnvironmentDiscovery;
      yield* Stream.merge(
        SubscriptionRef.changes(discovery.state).pipe(Stream.map(() => true)),
        SubscriptionRef.changes(registry.entries).pipe(Stream.map(() => false)),
      ).pipe(
        Stream.runForEach((discoveryChanged) =>
          Effect.gen(function* () {
            for (const entry of (yield* SubscriptionRef.get(
              discovery.state,
            )).environments.values()) {
              const descriptor = Option.getOrNull(entry.status)?.descriptor;
              if (descriptor !== undefined) {
                const error = orchestrationProtocolCompatibilityError(descriptor);
                // A cached relay response must not clear a newer socket preflight rejection.
                if (error !== null || discoveryChanged) {
                  yield* registry.setCompatibility(entry.environment.environmentId, error);
                }
              }
            }
          }).pipe(
            Effect.catch((error) =>
              Effect.logWarning("Could not apply discovered environment compatibility.", { error }),
            ),
          ),
        ),
        Effect.forkScoped,
      );
      yield* registry.start;
      yield* platformSource.registrations.pipe(
        Stream.runForEach(registry.reconcilePlatform),
        Effect.forkScoped,
      );
    }).pipe(Effect.withSpan("clientRuntime.connection.application.start")),
  );
  return connectionStartupLayer.pipe(
    Layer.provideMerge(connectionServicesLayer),
    Layer.provideMerge(RemoteEnvironmentAuthorization.layer),
  );
}

export const layer = layerWithOptions({});

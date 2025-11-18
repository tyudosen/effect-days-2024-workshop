import { Layer, Effect, Match, pipe } from "effect";
import { HttpServer as _HttpServer } from "@effect/platform";
import * as S from "./shared.ts";
import * as HttpServer from "./http.ts";
import { NodeRuntime } from "@effect/platform-node";


const StartMessage = Layer.effectDiscard(Effect.gen(function* () {
  const http_server = yield* _HttpServer.HttpServer;


  const httpPort = Match.type<typeof http_server.address>().pipe(
    Match.tag('TcpAddress', (port) => port.port),
    Match.tag('UnixAddress', (port) => port.path),
    Match.exhaustive
  )(http_server.address)

  yield* Effect.log(`HTTP server listening on port ${httpPort}`)
  yield* Effect.log(`WebSocket server ready (noServer mode)`);

})).pipe(
  Layer.provide(HttpServer.Http),
  Layer.provide(S.HttpServer.Live),
)

const MainLayer = Layer.mergeAll(
  HttpServer.Live,
  StartMessage,
  S.CurrentConnections.Live,
  S.MessageBroadcast.Live
);

pipe(MainLayer, Layer.launch, NodeRuntime.runMain);

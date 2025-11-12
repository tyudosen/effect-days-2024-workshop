import { Config, Context, Effect, Layer } from "effect";
import { createServer } from 'node:http'
import WebSocket, { WebSocketServer } from 'ws';

export class HttpServer extends Context.Tag('Http')<
  HttpServer,
  ReturnType<typeof createServer>
>() {
  static readonly Live = Layer.sync(HttpServer, createServer)
}


export class WsServer extends Context.Tag('WsServer')<
  WsServer,
  WebSocketServer
>() {
  static readonly Live = Layer.effect(
    WsServer,
    Effect.gen(function* () {

      const server = yield* HttpServer;
      const wss = new WebSocketServer({ server });

      return wss
    })
  )
    .pipe(
      Layer.provide(HttpServer.Live)
    )
}


export const Listen = Layer.effectDiscard(Effect.gen(function* () {
  const port = yield* Config.integer('PORT').pipe(
    Config.withDefault(3000),
    Config.withDescription('Port the server is running on')
  )
  const server = yield* HttpServer;

  yield* Effect.sync(() => server.listen(port, () => console.log("Server started on port 3000"))
  )
}))
  .pipe(
    Layer.provide(HttpServer.Live)
  )


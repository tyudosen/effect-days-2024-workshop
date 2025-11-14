import { Effect, Layer, Match, pipe } from "effect";
import * as S from "./shared.ts";
import * as M from "./model.ts";
import * as C from './config.ts'
import { HttpMiddleware, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform"
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node"

export const Http = Layer.scoped(
  HttpServer.HttpServer,
  S.HttpServer.pipe(
    Effect.zip(C.PORT),
    Effect.flatMap(([server, port]) =>
      NodeHttpServer.make(
        () => server,
        { port }
      ))
  )
)


export const Live = HttpRouter.empty.pipe(
  HttpRouter.get(
    '/',
    Effect.gen(function* (_) {
      const socket = yield* HttpServerRequest.upgrade
      const write = yield* socket.writer
      // const decoder = new TextDecoder()

      yield* socket.run((chunk) => {
        // const text = decoder.decode(chunk)
        return write(chunk)
      })

      return HttpServerResponse.empty()
    }),
  ),
  HttpRouter.get('/colors', Effect.gen(function* () {
    const colors = yield* S.getAvailableColors

    return yield* pipe(
      M.AvailableColorsResponse.make({
        colors
      }),
      HttpServerResponse.schemaJson(M.AvailableColorsResponse)
    )
  })),
  HttpServer.serve(HttpMiddleware.logger),
  HttpServer.withLogAddress
).pipe(
  Layer.provide(Http),
  Layer.provide(S.CurrentConnections.Live),
  Layer.provide(S.HttpServer.Live)
)



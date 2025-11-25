import { HttpMiddleware, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse, Socket } from "@effect/platform"
import { NodeHttpServer } from "@effect/platform-node"
import {
  Chunk,
  Console,
  Effect,
  Fiber,
  HashMap,
  Layer,
  Option,
  pipe,
  PubSub,
  Ref,
  Schema,
  Stream,
} from "effect"
import * as C from "./config.ts"
import * as M from "./model.ts"
import * as S from "./shared.ts"

export const Http = Layer.scoped(
  HttpServer.HttpServer,
  S.HttpServer.pipe(
    Effect.zip(C.PORT),
    Effect.flatMap(([server, port]) =>
      NodeHttpServer.make(
        () => server,
        { port }
      )
    )
  )
)

const parseOutgoingMessage = pipe(
  Schema.parseJson(M.ServerOutgoingMessage),
  Schema.encode
)

const parseMessage = pipe(
  Schema.parseJson(M.ServerIncomingMessage),
  Schema.decode
)

const parseStartupMessage = pipe(
  Schema.parseJson(M.StartupMessage),
  Schema.decode
)

const handleStartupMessage = (message: M.StartupMessage) =>
  Effect.gen(function* () {
    const current_connections_ref = yield* S.CurrentConnections
    const current_connections = yield* Ref.get(current_connections_ref)

    const socket = yield* HttpServerRequest.upgrade
    const socket_write = yield* socket.writer

    const messageBroadcast = yield* S.MessageBroadcast
    const { color, name } = message

    if (!M.colors.includes(color) || HashMap.has(current_connections, name)) {
      yield* socket_write(new Socket.CloseEvent(1008, "Color unavailable or name taken"))
      return
    }

    yield* Ref.update(current_connections_ref, (connections) =>
      HashMap.set(connections, name, {
        name,
        color,
        timeConnected: Date.now()
      }))

    yield* PubSub.publish(messageBroadcast, M.Join.make({ name, color }))
  }).pipe(
    Effect.scoped
  )

const handleMessage = (message: M.ServerIncomingMessage, connectionName: string | undefined) =>
  Effect.gen(function* () {
    const current_connections_ref = yield* S.CurrentConnections
    const messageBroadcast = yield* S.MessageBroadcast
    const connectionNameOption = Option.fromNullable(connectionName)

    yield* Effect.matchEffect(connectionNameOption, {
      onSuccess: (connName) =>
        Effect.gen(function* () {
          const current_connections = yield* Ref.get(current_connections_ref)

          const conn = HashMap.get(current_connections, connName).pipe(
            Option.getOrElse(() => undefined)
          )

          if (conn) {
            yield* PubSub.publish(
              messageBroadcast,
              M.Message.make({
                name: conn.name,
                color: conn.color,
                message: message.message,
                timestamp: Date.now()
              })
            )
          }
        }),
      onFailure: (_) => Console.error(_.toJSON())
    })
  })


export const Live = HttpRouter.empty.pipe(
  HttpRouter.get(
    "/",
    Effect.gen(function* () {
      const socket = yield* HttpServerRequest.upgrade
      const socket_write = yield* socket.writer
      const messageBroadcast = yield* S.MessageBroadcast
      let connectionName: string | undefined

      const broadcastFiber = yield* Stream.fromPubSub(messageBroadcast).pipe(
        Stream.filter((message) => {
          return !(connectionName && message.name === connectionName &&
            (message._tag === "message" || message._tag === "join"))
        }),
        Stream.mapEffect(parseOutgoingMessage),
        Stream.mapEffect((message) =>
          Effect.gen(function* () {
            const encodedMessage = new TextEncoder().encode(message)
            yield* socket_write(encodedMessage)
          })
        ),
        Stream.runDrain,
        Effect.fork
      )

      const [startupStream, messageStream] = yield* Stream.asyncEffect<Uint8Array<ArrayBufferLike>, Error>((emit) =>
        socket.run((chunk) => Effect.sync(() => emit(Effect.succeed(Chunk.of(chunk))))).pipe(
          Effect.catchAll((error) => Effect.sync(() => emit(Effect.fail(Option.some(error))))),
          Effect.fork
        )
      ).pipe(
        Stream.broadcast(2, 5)
      )

      const startMessageStream = pipe(
        startupStream,
        Stream.take(1),
        Stream.decodeText(),
        Stream.mapEffect(parseStartupMessage),
        Stream.mapEffect((message) =>
          handleStartupMessage(message).pipe(
            Effect.tap(() => Effect.sync(() => {
              connectionName = message.name
            }))
          )
        )
      )

      const incomingMessageStream = pipe(
        messageStream,
        Stream.drop(1),
        Stream.decodeText(),
        Stream.mapEffect(parseMessage),
        Stream.mapEffect((message) => handleMessage(message, connectionName))
      )

      yield* pipe(
        startMessageStream,
        Stream.runDrain,
        Effect.fork
      )


      const messageFiber = yield* pipe(
        incomingMessageStream,
        Stream.runDrain,
        Effect.fork
      )

      yield* Fiber.join(
        Fiber.zip(
          broadcastFiber,
          messageFiber
        )
      )

      return yield* HttpServerResponse.empty()
    })
  ),
  HttpRouter.get(
    "/colors",
    Effect.gen(function* () {
      const colors = yield* S.getAvailableColors

      return yield* pipe(
        M.AvailableColorsResponse.make({
          colors
        }),
        HttpServerResponse.schemaJson(M.AvailableColorsResponse)
      )
    })
  ),
  HttpServer.serve(HttpMiddleware.logger),
  HttpServer.withLogAddress
).pipe(
  Layer.provide(S.CurrentConnections.Live),
  Layer.provide(Http),
  Layer.provide(S.HttpServer.Live),
  Layer.provide(S.MessageBroadcast.Live)
)


import {
  Effect,
  HashMap,
  Layer,
  Match,
  Option,
  pipe,
  PubSub,
  Queue,
  Ref,
  Schema,
} from "effect";
import * as S from "./shared.ts";
import * as M from "./model.ts";
import * as C from './config.ts'
import {
  HttpMiddleware,
  HttpRouter,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
  Socket
} from "@effect/platform"
import { NodeHttpServer } from "@effect/platform-node"

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
    Effect.gen(function* () {
      const socket = yield* HttpServerRequest.upgrade
      const socket_write = yield* socket.writer
      const decoder = new TextDecoder()
      const current_connections_ref = yield* S.CurrentConnections;
      const messageBroadcast = yield* S.MessageBroadcast;
      let connectionName: string | undefined;

      const messageSubscription = yield* PubSub.subscribe(messageBroadcast);

      yield* Effect.fork(Effect.gen(function* () {
        while (true) {
          const message = yield* Queue.take(messageSubscription);

          if (connectionName && message.name === connectionName &&
            (message._tag === 'message' || message._tag === 'join')) {
            continue;
          }

          const messageString = JSON.stringify(message);
          const encodedMessage = new TextEncoder().encode(messageString);
          yield* socket_write(encodedMessage).pipe(
            Effect.catchAll(() => Effect.log('Failed to send message to client'))
          );
        }
      })).pipe(
        Effect.interruptible
      );


      yield* socket.run((chunk) => Effect.gen(function* () {
        const text = decoder.decode(chunk)
        const message = JSON.parse(text)

        const parsedMessage = Schema.decodeUnknownSync(
          Schema.Union(M.ServerIncomingMessage, M.StartupMessage),
        )(message);


        yield* Match.value(parsedMessage).pipe(
          Match.tag('startup', (message) => Effect.gen(function* () {
            const { color, name } = message;
            const current_connections = yield* Ref.get(current_connections_ref)


            if (!M.colors.includes(color) || HashMap.has(current_connections, name)) {
              yield* socket_write(new Socket.CloseEvent(1008, "Color unavailable or name taken"))
              return;
            }

            connectionName = name;


            yield* Ref.update(current_connections_ref, (connections) =>
              HashMap.set(connections, name, {
                name,
                color,
                timeConnected: Date.now(),
              })
            )

            yield* PubSub.publish(messageBroadcast, M.Join.make({ name, color }));
          })),
          Match.tag('message', (message) => Effect.gen(function* () {
            if (connectionName) {
              const current_connections = yield* Ref.get(current_connections_ref)


              const conn = HashMap.get(current_connections, connectionName).pipe(
                Option.getOrElse(() => undefined)
              );



              if (conn) {
                yield* PubSub.publish(
                  messageBroadcast,
                  M.Message.make({
                    name: conn.name,
                    color: conn.color,
                    message: message.message,
                    timestamp: Date.now(),
                  })
                );
              }
            }

          })),
          Match.exhaustive
        )


      }), {
        onOpen: Effect.log("WebSocket connection opened")
      })

      yield* Effect.addFinalizer(Effect.fn(function* () {
        yield* Match.type<typeof connectionName>().pipe(
          Match.when(Match.string, (name) => Effect.gen(function* () {
            const current_connections = yield* Ref.get(current_connections_ref);
            const conn = HashMap.get(current_connections, name);

            yield* Option.match(conn, {
              onSome: (conn) => PubSub.publish(messageBroadcast, M.Leave.make({
                name: conn.name,
                color: conn.color
              })),
              onNone: () => Effect.log('no conn')
            });

            yield* Ref.update(current_connections_ref, (connections) => HashMap.remove(connections, name));
          })),
          Match.when(Match.undefined, () => Effect.log('no connection')),
          Match.exhaustive
        )(connectionName).pipe(
          Effect.catchAll(() => Effect.log('leave broadcast failed'))
        );
      }))

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
  Layer.provide(S.MessageBroadcast.Live),
  Layer.provide(S.HttpServer.Live)
)

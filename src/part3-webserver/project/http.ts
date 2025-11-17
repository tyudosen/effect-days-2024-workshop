import {
  Effect,
  Layer,
  Match,
  pipe,
  Schema
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
      const current_connections = yield* S.CurrentConnections;
      let connectionName: string;

      const broadcastMessage = Effect.fnUntraced(function* (message: M.ServerOutgoingMessage) {
        const messageString = JSON.stringify(message);
        const encodedMessage = new TextEncoder().encode(messageString);


        yield* Effect.forEach(current_connections, ([_key, conn]) => {
          const matcher = Match.type<typeof conn>().pipe(
            Match.when(
              {
                write: (write: typeof conn.write) => Boolean(write),
                name: (name) => name !== connectionName
              },
              (conn) => conn.write(encodedMessage)
            ),
            Match.orElse((_conn) => Effect.log('Exempt from this brodcast'))
          );

          return matcher(conn)
        })
      })


      yield* socket.run((chunk) => Effect.gen(function* () {
        const text = decoder.decode(chunk)
        const message = JSON.parse(text)

        const parsedMessage = Schema.decodeUnknownSync(
          Schema.Union(M.ServerIncomingMessage, M.StartupMessage),
        )(message);


        yield* Match.value(parsedMessage).pipe(
          Match.tag('startup', (message) => Effect.gen(function* () {
            const { color, name } = message;

            if (!M.colors.includes(color) || current_connections.has(name)) {
              yield* socket_write(new Socket.CloseEvent(1008, "Color unavailable or name taken"))
              return;
            }

            connectionName = name;


            current_connections.set(name, {
              write: socket_write,
              socket,
              name,
              color,
              timeConnected: Date.now(),
            });

            yield* broadcastMessage(M.Join.make({ name, color }));
          })),
          Match.tag('message', (message) => Effect.gen(function* () {
            if (connectionName) {
              const conn = current_connections.get(connectionName);

              if (conn) {
                yield* broadcastMessage(
                  M.Message.make({
                    name: conn.name,
                    color: conn.color,
                    message: message.message,
                    timestamp: Date.now(),
                  }),
                );
              }
            }

          })),
          Match.exhaustive
        )


      }), {
        onOpen: Effect.log("WebSocket connection opened")
      })

      // Clean up connection when socket closes
      // yield* Effect.addFinalizer(() =>
      //   Effect.sync(() => {
      //     if (connectionName && current_connections.has(connectionName)) {
      //       const conn = current_connections.get(connectionName)!
      //       current_connections.delete(connectionName);
      //       broadcastMessage(M.Leave.make({ name: connectionName, color: conn?.color }));
      //     }
      //   })
      // )

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

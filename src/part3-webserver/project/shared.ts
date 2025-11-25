import { Context, Effect, HashMap, Layer, PubSub, Ref } from "effect"
import { createServer } from "node:http"
import * as M from "./model.ts"

export class HttpServer extends Context.Tag("Http")<
  HttpServer,
  ReturnType<typeof createServer>
>() {
  static readonly Live = Layer.sync(HttpServer, createServer)
}

export class CurrentConnections extends Context.Tag("CurrentConnections")<
  CurrentConnections,
  Ref.Ref<HashMap.HashMap<string, M.WebSocketConnection>>
>() {
  static readonly Live = Layer.effect(
    CurrentConnections,
    Ref.make(HashMap.empty<string, M.WebSocketConnection>())
  )
}

export class MessageBroadcast extends Context.Tag("MessageBroadcast")<
  MessageBroadcast,
  PubSub.PubSub<M.ServerOutgoingMessage>
>() {
  static readonly Live = Layer.effect(
    MessageBroadcast,
    PubSub.bounded<M.ServerOutgoingMessage>(100)
  )
}

export const getAvailableColors = Effect.gen(function* () {
  const current_connections_ref = yield* CurrentConnections
  const current_connections = yield* Ref.get(current_connections_ref)

  const currentColors = Array.from(HashMap.values(current_connections)).map(
    (conn) => conn.color
  )

  const availableColors = M.colors.filter(
    (color) => !currentColors.includes(color)
  )

  return availableColors
}).pipe(
  Effect.provide(CurrentConnections.Live)
)


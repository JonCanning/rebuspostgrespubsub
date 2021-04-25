open System.Threading.Tasks
open System
open System.Net.Http
open Rebus.Activation
open Rebus.Config
open Rebus.Handlers
open Rebus.Retry.Simple
open FSharp.Control.Tasks
open Rebus.Bus
open Rebus.Exceptions

let connectionString =
  "Server=localhost;Port=5432;Database=test;User Id=postgres;Password=password;maximum pool size=30"

type Message = { Body: string }

let createPublisher name =
  let activator = new BuiltinHandlerActivator()

  Configure
    .With(activator)
    .Transport(fun t ->
      t.UsePostgreSql(connectionString, "publisher", "publisher")
      |> ignore)
    .Subscriptions(fun s -> s.StoreInPostgres(connectionString, "subscriptions", true))
    .Start()

let createSubscriber name topic handler =
  let activator = new BuiltinHandlerActivator()

  let bus =
    Configure
      .With(activator)
      .Options(fun o -> o.SimpleRetryStrategy(name + "-poison", 1, secondLevelRetriesEnabled = true))
      .Transport(fun t ->
        t.UsePostgreSql(connectionString, name, name)
        |> ignore)
      .Subscriptions(fun s -> s.StoreInPostgres(connectionString, "subscriptions", true))
      .Start()

  bus.Advanced.Workers.SetNumberOfWorkers 0

  activator.Register(fun () -> handler (name, bus))
  |> ignore

  bus.Advanced.Workers.SetNumberOfWorkers Options.DefaultNumberOfWorkers
  bus.Advanced.Topics.Subscribe topic

let retryIntervals =
  [| 10; 20 |]
  |> Array.map (float >> TimeSpan.FromSeconds)

type Handler(name, bus: IBus) =
  interface IHandleMessages<Message> with
    override _.Handle message =
      unitTask {
        printfn "Start %s:%s" name message.Body
        failwith "bang"
        printfn "Done %s:%s" name message.Body
      }

  interface IHandleMessages<IFailed<Message>> with
    override _.Handle failedMessage =
      unitTask {
        let deferCount =
          match failedMessage.Headers.TryGetValue Rebus.Messages.Headers.DeferCount with
          | true, value ->
              int value
              |> Some
              |> Option.filter (fun i -> i < retryIntervals.Length)
          | _ -> None

        match deferCount with
        | Some deferCount ->
            return!
              retryIntervals
              |> Array.tryItem (deferCount - 1)
              |> Option.map bus.Advanced.TransportMessage.Defer
              |> Option.defaultValue Task.CompletedTask
        | _ -> return! bus.Advanced.TransportMessage.Deadletter failedMessage.ErrorDescription
      }

(createSubscriber "subscriber" "topic" Handler)
  .Wait()

let publisher = createPublisher "publisher"

publisher
  .Advanced
  .Topics
  .Publish("topic", { Body = "BODY" })
  .Wait()

Console.ReadLine() |> ignore

//------------------------------------------
let title = "Functional Event Sourcing"
let speaker = "Jérémie Chassaing"
let company = "d-edge / Availpro Hospitality Tech"

let twitter = @"thinkb4coding"
let blog = "https://thinkbeforecoding.com"
//------------------------------------------
System.Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
#load ".paket/load/netstandard2.0/main.group.fsx"

type Command =
    | SwitchOn
    | SwitchOff
type Event = 
    | SwitchedOn
    | SwitchedOff
    | Broke

type Status =
    | On
    | Off

type Working =
    { Status : Status
      RemainingUses: int}

    
type State =
    | Working of Working
    | Broken
let initialState = Working { Status = Off; RemainingUses = 3}

let decide (command: Command) (state: State) : Event list =
    match state, command with
    | Working { Status = Off; RemainingUses = 0 }, SwitchOn -> 
        [ Broke]
    | Working { Status = Off}, SwitchOn -> [ SwitchedOn]
    |  Working { Status = On }, SwitchOff -> [SwitchedOff]
    | _ -> []

let evolve (state: State) (event: Event) : State =
    match state, event with
    | _, Broke -> Broken
    | Working s, SwitchedOn -> 
        Working { Status = On; 
                  RemainingUses = s.RemainingUses - 1 }
    | Working s, SwitchedOff ->
        Working { s with Status = Off}
    | _ -> state

let (=>) events cmd =
    events
    |> List.fold evolve initialState
    |> decide cmd

let (==) = (=)

[]
=> SwitchOn
== [ SwitchedOn ]


[ SwitchedOn]
=> SwitchOn
== [ ]

[]
=> SwitchOff
== []

[ SwitchedOn]
=> SwitchOff
== [ SwitchedOff ]


[ SwitchedOn
  SwitchedOff
  SwitchedOn
  SwitchedOff
  SwitchedOn
  SwitchedOff]
=> SwitchOn
== [ Broke ]


module Event =
    let deserialize input =
        match input with
        | "SwitchedOn" -> [SwitchedOn]
        | "SwitchedOff" -> [SwitchedOff]
        | "Broke" -> [Broke]
        | _ -> []

    let serialize event =
        match event with
        | SwitchedOn -> "SwitchedOn"
        | SwitchedOff -> "SwitchedOff"
        | Broke -> "Broke"

module State =
    let deserialize (input: string) =
        
        match input.Split(';') with
        | [|"On"; n|] -> Some (Working {Status = On; RemainingUses = int n })
        | [|"Off"; n|] -> Some (Working {Status = Off; RemainingUses = int n })
        | [|"Broken"|] -> Some Broken
        | _ -> None

    let serialize state =
        match state with
        | Working { Status = On; RemainingUses = n } ->
            sprintf "On;%d" n 
        | Working { Status = Off; RemainingUses = n } -> 
            sprintf "Off;%d" n 
        | Broken -> "Broken"



let start publish =
    let mutable state = initialState
    fun cmd ->
        let events = decide cmd state
        let newState = List.fold evolve state events
        state <- newState

        for event in events do
            publish event

let light = start (printfn "%A")

light SwitchOn
light SwitchOff

module Store =
    open System.IO
    let loadState name =
        if File.Exists name then
            match File.ReadAllLines name with
            | [| line |] -> State.deserialize line
            | _ -> None
        else
            None
    let saveState name state =
        File.WriteAllLines(name, [| State.serialize state |])

    let load name version =
        if File.Exists name then
            let lines = File.ReadAllLines name
            let events =
                lines
                |> Seq.skip version
                |> Seq.collect (Event.deserialize)
               
                |> Seq.toList
            lines.Length, events
        else
            0, []

    let save name expectedVersion events =
        let lines =
            if File.Exists name then
                (File.ReadAllLines name).Length
            else
                0
        if lines <> expectedVersion then
            failwith "Wrong expected version"
        

        File.AppendAllLines(name, List.map Event.serialize events )
        lines + events.Length

let start' name publish =
    fun cmd ->
        let version,s = 
            Store.loadState name 
            |> Option.defaultValue initialState
        let events = decide cmd state
        let newState = List.fold evolve state events
        Store.saveState name newState


        for event in events do
            publish event


let light1 = start' "light1" (printfn "%A")

light1 SwitchOn
light1 SwitchOff


module EventStore =
    open System
    open EventStore.ClientAPI
    let store =
        let cnx = EventStoreConnection.Create(Uri "tcp://127.0.0.1:1113")
        cnx.ConnectAsync() |> Async.AwaitTask |> Async.RunSynchronously
        cnx
    let cred = SystemData.UserCredentials("admin","changeit")
    let load stream start =
        let slice = store.ReadStreamEventsForwardAsync(stream, int64 start, 4096, true, cred ) 
                    |> Async.AwaitTask |> Async.RunSynchronously
        let events =
            slice.Events
            |> Seq.collect (fun e -> Event.deserialize e.Event.EventType)
            |> Seq.toList

        int slice.NextEventNumber, events

        
    let save stream expectedVersion events =
        if List.isEmpty events then
            (expectedVersion)
        else
            let eventsData =
                [| for e in events ->
                    EventData(Guid.NewGuid(), Event.serialize e , true, "{}"B, null) |]
            let result =
                store.ConditionalAppendToStreamAsync(stream, int64 expectedVersion, eventsData)
                |> Async.AwaitTask |> Async.RunSynchronously
            if result.Status = ConditionalWriteStatus.Succeeded then
                (int result.NextExpectedVersion.Value)
            else
                failwith "Wrong version"

let start'' name publish =
    let v,events = EventStore.load name 0
    let mutable state =
        events
        |> List.fold evolve initialState

    let mutable version = v
    fun cmd ->


        let mutable saved = false
        while not saved do
            let events = decide cmd state
            try
                version <- EventStore.save name version events
                state <- List.fold evolve state events
                saved <- true
            with
            | _ -> 
                let v,events = EventStore.load name version
                version <- v
                state <- List.fold evolve state events

        for event in events do
            publish event


let light3 = start'' "light3" (printfn "%A")

light3 SwitchOn
light3 SwitchOff

       
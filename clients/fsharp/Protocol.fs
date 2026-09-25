namespace NetworkMutex

open System
open System.Collections.Generic
open System.Text.Json

type RequestType =
  | Version
  | Auth
  | Lock
  | Unlock
  | RegisterRead
  | RegisterWrite
  | EndRead
  | EndWrite
  | LockInfo
  | Ls
  | Heartbeat

type ResponseType =
  | Version
  | Auth
  | Lock
  | CompositeLock
  | Unlock
  | RegisterReadResult
  | RegisterWriteResult
  | EndReadResult
  | EndWriteResult
  | LockInfo
  | LsResult
  | Reelection
  | Error
  | Ok
  | Unknown

type Response =
  { Type: ResponseType
    Uuid: string
    Key: string option
    Keys: string list
    Acquired: bool option
    Unlocked: bool option
    Granted: bool option
    LockUuid: string option
    FencingToken: uint64 option
    FencingTokens: Map<string, uint64>
    Error: string option }

module Protocol =
  [<Literal>]
  let ProtocolVersion = "0.1.0"

  [<Literal>]
  let MaxCompositeKeys = 5

  [<Literal>]
  let MaxFencingToken = 9_007_199_254_740_991UL

  let requestTypes =
    [ "version"
      "auth"
      "lock"
      "unlock"
      "registerRead"
      "registerWrite"
      "endRead"
      "endWrite"
      "lockInfo"
      "ls"
      "heartbeat" ]

  let responseTypes =
    [ "version"
      "auth"
      "lock"
      "compositeLock"
      "unlock"
      "registerReadResult"
      "registerWriteResult"
      "endReadResult"
      "endWriteResult"
      "lockInfo"
      "lsResult"
      "reelection"
      "error"
      "ok" ]

  let requestTypeToWire =
    function
    | RequestType.Version -> "version"
    | RequestType.Auth -> "auth"
    | RequestType.Lock -> "lock"
    | RequestType.Unlock -> "unlock"
    | RequestType.RegisterRead -> "registerRead"
    | RequestType.RegisterWrite -> "registerWrite"
    | RequestType.EndRead -> "endRead"
    | RequestType.EndWrite -> "endWrite"
    | RequestType.LockInfo -> "lockInfo"
    | RequestType.Ls -> "ls"
    | RequestType.Heartbeat -> "heartbeat"

  let responseTypeFromWire =
    function
    | "version" -> ResponseType.Version
    | "auth" -> ResponseType.Auth
    | "lock" -> ResponseType.Lock
    | "compositeLock" -> ResponseType.CompositeLock
    | "unlock" -> ResponseType.Unlock
    | "registerReadResult" -> ResponseType.RegisterReadResult
    | "registerWriteResult" -> ResponseType.RegisterWriteResult
    | "endReadResult" -> ResponseType.EndReadResult
    | "endWriteResult" -> ResponseType.EndWriteResult
    | "lockInfo" -> ResponseType.LockInfo
    | "lsResult" -> ResponseType.LsResult
    | "reelection" -> ResponseType.Reelection
    | "error" -> ResponseType.Error
    | "ok" -> ResponseType.Ok
    | _ -> ResponseType.Unknown

  let private frame (fields: (string * obj option) list) =
    let dict = Dictionary<string, obj>()

    for key, value in fields do
      match value with
      | Some v -> dict[key] <- v
      | None -> ()

    JsonSerializer.Serialize(dict) + "\n"

  let versionRequest uuid value =
    frame [ "type", Some(box "version"); "uuid", Some(box uuid); "value", Some(box value) ]

  let authRequest uuid token =
    frame [ "type", Some(box "auth"); "uuid", Some(box uuid); "token", Some(box token) ]

  let lockRequestSingle uuid key ttlMs maxHolders wait =
    frame
      [ "type", Some(box "lock")
        "uuid", Some(box uuid)
        "key", Some(box key)
        "ttl", (if ttlMs > 0 then Some(box ttlMs) else None)
        "max", Option.map box maxHolders
        "wait", Option.map box wait ]

  let lockRequestComposite uuid keys ttlMs wait =
    let count = List.length keys

    if count < 1 || count > MaxCompositeKeys then
      invalidArg (nameof keys) $"composite key count must be 1..=5, got {count}"

    frame
      [ "type", Some(box "lock")
        "uuid", Some(box uuid)
        "keys", Some(box (keys |> List.toArray))
        "ttl", (if ttlMs > 0 then Some(box ttlMs) else None)
        "wait", Option.map box wait ]

  let unlockRequestSingle uuid key lockUuid force =
    frame
      [ "type", Some(box "unlock")
        "uuid", Some(box uuid)
        "key", Some(box key)
        "lockUuid", (lockUuid |> Option.filter (fun s -> s <> "") |> Option.map box)
        "force", (if force then Some(box true) else None) ]

  let unlockRequestComposite uuid keys lockUuid =
    frame
      [ "type", Some(box "unlock")
        "uuid", Some(box uuid)
        "keys", Some(box (keys |> List.toArray))
        "lockUuid", (lockUuid |> Option.filter (fun s -> s <> "") |> Option.map box) ]

  let rwRequest requestType uuid key =
    frame [ "type", Some(box (requestTypeToWire requestType)); "uuid", Some(box uuid); "key", Some(box key) ]

  let lockInfoRequest uuid key = rwRequest RequestType.LockInfo uuid key
  let lsRequest uuid = frame [ "type", Some(box "ls"); "uuid", Some(box uuid) ]
  let heartbeatRequest uuid = frame [ "type", Some(box "heartbeat"); "uuid", Some(box uuid) ]

  let private tryGetString (name: string) (root: JsonElement) =
    match root.TryGetProperty name with
    | true, value when value.ValueKind = JsonValueKind.String -> Some(value.GetString())
    | _ -> None

  let private tryGetBool (name: string) (root: JsonElement) =
    match root.TryGetProperty name with
    | true, value when value.ValueKind = JsonValueKind.True || value.ValueKind = JsonValueKind.False ->
      Some(value.GetBoolean())
    | _ -> None

  let private tryGetUInt64 (name: string) (root: JsonElement) =
    match root.TryGetProperty name with
    | true, value when value.ValueKind = JsonValueKind.Number ->
      match value.TryGetUInt64() with
      | true, n -> Some n
      | _ -> None
    | _ -> None

  let private getStringList (name: string) (root: JsonElement) =
    match root.TryGetProperty name with
    | true, value when value.ValueKind = JsonValueKind.Array ->
      value.EnumerateArray()
      |> Seq.map (fun item -> item.GetString())
      |> Seq.filter (fun item -> not (isNull item))
      |> Seq.toList
    | _ -> []

  let private getUInt64Map (name: string) (root: JsonElement) =
    match root.TryGetProperty name with
    | true, value when value.ValueKind = JsonValueKind.Object ->
      value.EnumerateObject()
      |> Seq.choose (fun prop ->
        match prop.Value.TryGetUInt64() with
        | true, n -> Some(prop.Name, n)
        | _ -> None)
      |> Map.ofSeq
    | _ -> Map.empty

  let private validToken =
    function
    | Some token when token >= 1UL && token <= MaxFencingToken -> true
    | _ -> false

  let private validTokenValue token =
    token >= 1UL && token <= MaxFencingToken

  let private nonEmpty =
    function
    | Some value when not (String.IsNullOrWhiteSpace value) -> true
    | _ -> false

  let private validateAuthority response =
    match response.Type, response.Acquired, response.Granted with
    | ResponseType.Lock, Some true, _ ->
      if not (nonEmpty response.Key && nonEmpty response.LockUuid && validToken response.FencingToken) then
        raise (JsonException("successful lock grant omitted valid fenced authority"))
    | ResponseType.CompositeLock, Some true, _ ->
      let distinctKeys = response.Keys |> Set.ofList |> Set.count
      let completeTokens =
        response.Keys
        |> List.forall (fun key ->
          match Map.tryFind key response.FencingTokens with
          | Some token -> validTokenValue token
          | None -> false)

      if not (nonEmpty response.LockUuid
              && response.Keys.Length >= 1
              && response.Keys.Length <= MaxCompositeKeys
              && distinctKeys = response.Keys.Length
              && Map.count response.FencingTokens = response.Keys.Length
              && completeTokens) then
        raise (JsonException("successful composite grant omitted complete fenced authority"))
    | (ResponseType.RegisterReadResult | ResponseType.RegisterWriteResult), _, Some true ->
      if not (nonEmpty response.Key && nonEmpty response.LockUuid && validToken response.FencingToken) then
        raise (JsonException("successful reader/writer grant omitted valid fenced authority"))
    | _ -> ()

    response

  let decodeResponse (line: string) =
    use doc = JsonDocument.Parse(line)
    let root = doc.RootElement

    let response =
      { Type = responseTypeFromWire (tryGetString "type" root |> Option.defaultValue "")
        Uuid = tryGetString "uuid" root |> Option.defaultValue ""
        Key = tryGetString "key" root
        Keys = getStringList "keys" root
        Acquired = tryGetBool "acquired" root
        Unlocked = tryGetBool "unlocked" root
        Granted = tryGetBool "granted" root
        LockUuid = tryGetString "lockUuid" root
        FencingToken = tryGetUInt64 "fencingToken" root
        FencingTokens = getUInt64Map "fencingTokens" root
        Error = tryGetString "error" root }

    validateAuthority response

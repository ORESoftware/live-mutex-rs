# LiveMutexClient.ps1 — PowerShell client for the dd-rust-network-mutex broker.
#
# Mirrors the Bash / Python / Go clients: it speaks the newline-delimited JSON
# wire protocol in ../../PROTOCOL.md (canonical schema: ../../src/protocol.rs).
# Every wire `type` value is a named constant in $LmxReq / $LmxRes below instead
# of an inline magic string. Works on Windows PowerShell 5.1 and PowerShell 7+
# (pwsh) on macOS/Linux.
#
# Dot-source this file and use [LiveMutexClient]::Connect(host, port); see
# smoke.ps1 for an end-to-end example.

# StrictMode 1.0 still catches uninitialized variables but lets an absent JSON
# field read back as $null so the client can reject malformed authority with a
# controlled protocol error instead of an unhelpful property-access failure.
Set-StrictMode -Version 1.0

$script:LmxReq = @{
    Version       = 'version'
    Auth          = 'auth'
    Lock          = 'lock'
    Unlock        = 'unlock'
    RegisterRead  = 'registerRead'
    RegisterWrite = 'registerWrite'
    EndRead       = 'endRead'
    EndWrite      = 'endWrite'
    LockInfo      = 'lockInfo'
    Ls            = 'ls'
    Heartbeat     = 'heartbeat'
}

$script:LmxRes = @{
    Version             = 'version'
    Auth                = 'auth'
    Lock                = 'lock'
    CompositeLock       = 'compositeLock'
    Unlock              = 'unlock'
    RegisterReadResult  = 'registerReadResult'
    RegisterWriteResult = 'registerWriteResult'
    EndReadResult       = 'endReadResult'
    EndWriteResult      = 'endWriteResult'
    LockInfo            = 'lockInfo'
    LsResult            = 'lsResult'
    Reelection          = 'reelection'
    Error               = 'error'
    Ok                  = 'ok'
}

class LiveMutexClient {
    [System.Net.Sockets.TcpClient] $Tcp
    [System.IO.StreamReader] $Reader
    [System.IO.Stream] $Stream
    [int] $TimeoutMs = 30000

    static [LiveMutexClient] Connect([string] $iHost, [int] $port) {
        return [LiveMutexClient]::Connect($iHost, $port, $null)
    }

    static [LiveMutexClient] Connect([string] $iHost, [int] $port, [string] $token) {
        $c = [LiveMutexClient]::new()
        $c.Tcp = [System.Net.Sockets.TcpClient]::new()
        $c.Tcp.NoDelay = $true
        $c.Tcp.Connect($iHost, $port)
        $c.Stream = $c.Tcp.GetStream()
        $c.Stream.ReadTimeout = $c.TimeoutMs
        $c.Reader = [System.IO.StreamReader]::new($c.Stream, [System.Text.Encoding]::UTF8)
        if ($token) {
            $u = [LiveMutexClient]::NewUuid()
            $r = $c.Roundtrip(@{ type = $script:LmxReq.Auth; uuid = $u; token = $token }, $u)
            if (-not $r.ok) {
                $c.Disconnect()
                throw "auth rejected: $($r | ConvertTo-Json -Compress)"
            }
        }
        return $c
    }

    static [string] NewUuid() {
        return [guid]::NewGuid().ToString()
    }

    hidden [void] Send([hashtable] $frame) {
        $json = ($frame | ConvertTo-Json -Compress -Depth 6) + "`n"
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($json)
        $this.Stream.Write($bytes, 0, $bytes.Length)
        $this.Stream.Flush()
    }

    hidden [object] ReadReply([string] $want) {
        while ($true) {
            $line = $this.Reader.ReadLine()
            if ($null -eq $line) {
                throw 'connection closed by broker'
            }
            if ($line -eq '') {
                continue
            }
            $obj = $line | ConvertFrom-Json
            if ($obj.uuid -eq $want) {
                return $obj
            }
        }
        throw 'unreachable'
    }

    hidden [object] ReadGrant([string] $want) {
        while ($true) {
            $obj = $this.ReadReply($want)
            if ($null -ne $obj.error) {
                return $obj
            }
            if ($obj.acquired -eq $true) {
                return $obj
            }
            if ($obj.acquired -eq $false) {
                continue
            }
            return $obj
        }
        throw 'unreachable'
    }

    hidden [object] ReadUntilGranted([string] $want) {
        while ($true) {
            $obj = $this.ReadReply($want)
            if ($obj.granted -eq $true) {
                return $obj
            }
            if ($null -ne $obj.error) {
                throw "rw acquire failed: $($obj.error)"
            }
        }
        throw 'unreachable'
    }

    hidden [object] Roundtrip([hashtable] $frame, [string] $uuid) {
        $this.Send($frame)
        return $this.ReadReply($uuid)
    }

    hidden [long] RequireToken([object] $value, [string] $context) {
        if ($null -eq $value) {
            throw "${context}: successful grant omitted fencing authority"
        }

        try {
            $token = [long]$value
        }
        catch {
            throw "${context}: fencing token is not an exact integer"
        }

        if ($token -lt 1 -or $token -gt 9007199254740991) {
            throw "${context}: fencing token is outside 1..=9007199254740991"
        }

        return $token
    }

    hidden [long] ValidateSingleGrant([object] $reply, [string] $expectedKey, [string] $context) {
        if ([string]::IsNullOrWhiteSpace([string]$reply.lockUuid)) {
            throw "${context}: successful grant omitted lockUuid"
        }
        if ($null -ne $reply.key -and [string]$reply.key -ne $expectedKey) {
            throw "${context}: broker returned authority for unexpected key"
        }
        return $this.RequireToken($reply.fencingToken, $context)
    }

    hidden [object] ValidateCompositeGrant([object] $reply, [string[]] $expectedKeys, [string] $context) {
        if ([string]::IsNullOrWhiteSpace([string]$reply.lockUuid)) {
            throw "${context}: successful grant omitted lockUuid"
        }
        if ($null -eq $reply.keys -or $null -eq $reply.fencingTokens) {
            throw "${context}: successful grant omitted composite authority"
        }

        $returnedKeys = @($reply.keys)
        if ($returnedKeys.Count -ne $expectedKeys.Count -or $returnedKeys.Count -lt 1 -or $returnedKeys.Count -gt 5) {
            throw "${context}: composite key cardinality mismatch"
        }
        if (@($returnedKeys | Select-Object -Unique).Count -ne $returnedKeys.Count) {
            throw "${context}: composite grant repeated a key"
        }
        foreach ($key in $expectedKeys) {
            if ($returnedKeys -notcontains $key) {
                throw "${context}: composite grant returned an unexpected key set"
            }
        }

        $properties = @($reply.fencingTokens.PSObject.Properties)
        if ($properties.Count -ne $returnedKeys.Count) {
            throw "${context}: fencing-token map cardinality mismatch"
        }
        foreach ($key in $returnedKeys) {
            $property = $reply.fencingTokens.PSObject.Properties[$key]
            if ($null -eq $property) {
                throw "${context}: missing fencing token for key $key"
            }
            $null = $this.RequireToken($property.Value, "$context/$key")
        }

        return $reply.fencingTokens
    }

    [pscustomobject] Acquire([string] $key, [int] $ttlMs) {
        $u = [LiveMutexClient]::NewUuid()
        $this.Send(@{ type = $script:LmxReq.Lock; uuid = $u; key = $key; ttl = $ttlMs; wait = $true })
        $r = $this.ReadGrant($u)
        if ($r.acquired -ne $true) {
            throw "acquire($key) failed: $($r | ConvertTo-Json -Compress)"
        }
        $token = $this.ValidateSingleGrant($r, $key, "acquire($key)")
        return [pscustomobject]@{ Key = $key; LockUuid = [string]$r.lockUuid; FencingToken = $token }
    }

    [pscustomobject] TryAcquire([string] $key, [int] $ttlMs) {
        $u = [LiveMutexClient]::NewUuid()
        $r = $this.Roundtrip(@{ type = $script:LmxReq.Lock; uuid = $u; key = $key; ttl = $ttlMs; wait = $false }, $u)
        if ($r.type -eq $script:LmxRes.Error) {
            throw "try_acquire($key) error: $($r.error)"
        }
        if ($r.acquired -ne $true) {
            return $null
        }
        $token = $this.ValidateSingleGrant($r, $key, "try_acquire($key)")
        return [pscustomobject]@{ Key = $key; LockUuid = [string]$r.lockUuid; FencingToken = $token }
    }

    [void] Release([string] $key, [string] $lockUuid) {
        $u = [LiveMutexClient]::NewUuid()
        $r = $this.Roundtrip(@{ type = $script:LmxReq.Unlock; uuid = $u; key = $key; lockUuid = $lockUuid }, $u)
        if ($r.unlocked -ne $true) {
            throw "release($key) failed: $($r | ConvertTo-Json -Compress)"
        }
    }

    [pscustomobject] AcquireMany([string[]] $keys, [int] $ttlMs) {
        if ($keys.Count -lt 1 -or $keys.Count -gt 5 -or @($keys | Select-Object -Unique).Count -ne $keys.Count) {
            throw 'acquire_many requires 1..=5 distinct keys'
        }
        $u = [LiveMutexClient]::NewUuid()
        $this.Send(@{ type = $script:LmxReq.Lock; uuid = $u; keys = $keys; ttl = $ttlMs; wait = $true })
        $r = $this.ReadGrant($u)
        if ($r.acquired -ne $true) {
            throw "acquire_many failed: $($r | ConvertTo-Json -Compress)"
        }
        $tokens = $this.ValidateCompositeGrant($r, $keys, 'acquire_many')
        return [pscustomobject]@{ Keys = @($r.keys); LockUuid = [string]$r.lockUuid; FencingTokens = $tokens }
    }

    [void] ReleaseMany([string[]] $keys, [string] $lockUuid) {
        $u = [LiveMutexClient]::NewUuid()
        $r = $this.Roundtrip(@{ type = $script:LmxReq.Unlock; uuid = $u; keys = $keys; lockUuid = $lockUuid }, $u)
        if ($r.unlocked -ne $true) {
            throw "release_many failed: $($r | ConvertTo-Json -Compress)"
        }
    }

    [pscustomobject] AcquireWrite([string] $key) {
        $u = [LiveMutexClient]::NewUuid()
        $this.Send(@{ type = $script:LmxReq.RegisterWrite; uuid = $u; key = $key })
        $r = $this.ReadUntilGranted($u)
        $token = $this.ValidateSingleGrant($r, $key, "acquire_write($key)")
        return [pscustomobject]@{ Key = $key; LockUuid = [string]$r.lockUuid; FencingToken = $token }
    }

    [pscustomobject] AcquireRead([string] $key) {
        $u = [LiveMutexClient]::NewUuid()
        $this.Send(@{ type = $script:LmxReq.RegisterRead; uuid = $u; key = $key })
        $r = $this.ReadUntilGranted($u)
        $token = $this.ValidateSingleGrant($r, $key, "acquire_read($key)")
        return [pscustomobject]@{ Key = $key; LockUuid = [string]$r.lockUuid; FencingToken = $token }
    }

    [void] ReleaseWrite([string] $key) {
        $u = [LiveMutexClient]::NewUuid()
        $this.Roundtrip(@{ type = $script:LmxReq.EndWrite; uuid = $u; key = $key }, $u) | Out-Null
    }

    [void] ReleaseRead([string] $key) {
        $u = [LiveMutexClient]::NewUuid()
        $this.Roundtrip(@{ type = $script:LmxReq.EndRead; uuid = $u; key = $key }, $u) | Out-Null
    }

    [string[]] Ls() {
        $u = [LiveMutexClient]::NewUuid()
        $r = $this.Roundtrip(@{ type = $script:LmxReq.Ls; uuid = $u }, $u)
        if ($null -eq $r.keys) {
            return @()
        }
        return $r.keys
    }

    [void] Disconnect() {
        if ($this.Reader) {
            $this.Reader.Dispose()
        }
        if ($this.Tcp) {
            $this.Tcp.Close()
        }
    }
}

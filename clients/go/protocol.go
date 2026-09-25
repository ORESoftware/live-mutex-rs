// Package networkmutex is the Go client for the live-mutex-rs broker.
package networkmutex

import (
	"encoding/json"
	"fmt"
)

const MaxFencingToken uint64 = 9007199254740991

type RequestType string

const (
	ReqVersion       RequestType = "version"
	ReqAuth          RequestType = "auth"
	ReqLock          RequestType = "lock"
	ReqUnlock        RequestType = "unlock"
	ReqRegisterRead  RequestType = "registerRead"
	ReqRegisterWrite RequestType = "registerWrite"
	ReqEndRead       RequestType = "endRead"
	ReqEndWrite      RequestType = "endWrite"
	ReqLockInfo      RequestType = "lockInfo"
	ReqLs            RequestType = "ls"
	ReqHeartbeat     RequestType = "heartbeat"
)

type ResponseType string

const (
	RespVersion             ResponseType = "version"
	RespAuth                ResponseType = "auth"
	RespLock                ResponseType = "lock"
	RespCompositeLock       ResponseType = "compositeLock"
	RespUnlock              ResponseType = "unlock"
	RespRegisterReadResult  ResponseType = "registerReadResult"
	RespRegisterWriteResult ResponseType = "registerWriteResult"
	RespEndReadResult       ResponseType = "endReadResult"
	RespEndWriteResult      ResponseType = "endWriteResult"
	RespLockInfo            ResponseType = "lockInfo"
	RespLsResult            ResponseType = "lsResult"
	RespReelection          ResponseType = "reelection"
	RespError               ResponseType = "error"
	RespOk                  ResponseType = "ok"
)

type Request struct {
	Type RequestType `json:"type"`
	UUID string      `json:"uuid"`
	Value string `json:"value,omitempty"`
	Token string `json:"token,omitempty"`
	Key string `json:"key,omitempty"`
	Keys []string `json:"keys,omitempty"`
	PID int `json:"pid,omitempty"`
	TTL int `json:"ttl,omitempty"`
	Max *int `json:"max,omitempty"`
	Force bool `json:"force,omitempty"`
	RetryCount int `json:"retryCount,omitempty"`
	KeepLocksAfterDeath bool `json:"keepLocksAfterDeath,omitempty"`
	Wait *bool `json:"wait,omitempty"`
	LockUUID string `json:"lockUuid,omitempty"`
}

type Response struct {
	Type ResponseType `json:"type"`
	UUID string `json:"uuid"`
	BrokerVersion string `json:"brokerVersion,omitempty"`
	OK *bool `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
	Key string `json:"key,omitempty"`
	Keys []string `json:"keys,omitempty"`
	Acquired *bool `json:"acquired,omitempty"`
	Unlocked *bool `json:"unlocked,omitempty"`
	LockRequestCount *int `json:"lockRequestCount,omitempty"`
	LockUUID string `json:"lockUuid,omitempty"`
	FencingToken *uint64 `json:"fencingToken,omitempty"`
	FencingTokens map[string]uint64 `json:"fencingTokens,omitempty"`
	ReadersCount *int `json:"readersCount,omitempty"`
	WriterFlag *bool `json:"writerFlag,omitempty"`
	Granted *bool `json:"granted,omitempty"`
	IsLocked *bool `json:"isLocked,omitempty"`
	LockholderUUIDs []string `json:"lockholderUuids,omitempty"`
}

func (r Request) Encode() ([]byte, error) {
	buf, err := json.Marshal(r)
	if err != nil { return nil, fmt.Errorf("encode request: %w", err) }
	return append(buf, '\n'), nil
}

func validFence(v uint64) bool { return v >= 1 && v <= MaxFencingToken }

func validateFencedAuthority(resp Response) error {
	if resp.Type == RespLock && resp.Acquired != nil && *resp.Acquired {
		if resp.LockUUID == "" || resp.FencingToken == nil || !validFence(*resp.FencingToken) {
			return fmt.Errorf("acquired lock missing exact fenced authority")
		}
	}
	if resp.Type == RespCompositeLock && resp.Acquired != nil && *resp.Acquired {
		if resp.LockUUID == "" || len(resp.Keys) == 0 || len(resp.FencingTokens) != len(resp.Keys) {
			return fmt.Errorf("acquired composite lock has incomplete fenced authority")
		}
		seen := make(map[string]struct{}, len(resp.Keys))
		for _, key := range resp.Keys {
			if _, duplicate := seen[key]; duplicate { return fmt.Errorf("duplicate composite key %q", key) }
			seen[key] = struct{}{}
			token, ok := resp.FencingTokens[key]
			if !ok || !validFence(token) { return fmt.Errorf("invalid fencing token for composite key %q", key) }
		}
	}
	if (resp.Type == RespRegisterReadResult || resp.Type == RespRegisterWriteResult) && resp.Granted != nil && *resp.Granted {
		if resp.LockUUID == "" || resp.FencingToken == nil || !validFence(*resp.FencingToken) {
			return fmt.Errorf("granted rw lock missing exact fenced authority")
		}
	}
	return nil
}

// Decode parses a single broker frame and rejects authority-bearing success
// frames that omit, zero, overflow, or incompletely map fencing tokens.
func Decode(buf []byte) (Response, error) {
	var resp Response
	if err := json.Unmarshal(buf, &resp); err != nil {
		return Response{}, fmt.Errorf("decode response: %w", err)
	}
	if err := validateFencedAuthority(resp); err != nil {
		return Response{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}

ExUnit.start()

defmodule NetworkMutex.ProtocolTest do
  use ExUnit.Case, async: true

  alias NetworkMutex.Protocol

  test "single lock request encodes camelCase fields and wait false" do
    frame = Protocol.lock_request_single("u-1", "k1", ttl_ms: 4000, max_holders: 1, wait: false)

    assert frame =~ "\"type\":\"lock\""
    assert frame =~ "\"key\":\"k1\""
    assert frame =~ "\"ttl\":4000"
    assert frame =~ "\"max\":1"
    assert frame =~ "\"wait\":false"
    assert String.ends_with?(frame, "\n")
  end

  test "composite lock request preserves key order and wait true" do
    frame = Protocol.lock_request_composite("u-2", ["c", "a", "b"], wait: true)

    assert frame =~ "\"keys\":[\"c\",\"a\",\"b\"]"
    assert frame =~ "\"wait\":true"
  end

  test "composite oversize is rejected" do
    assert_raise ArgumentError, fn ->
      Protocol.lock_request_composite("u", ["a", "b", "c", "d", "e", "f"])
    end
  end

  test "response discriminator mapping covers composite lock and ok" do
    assert Protocol.response_type_from_wire("compositeLock") == :composite_lock
    assert Protocol.response_type_from_wire("registerReadResult") == :register_read_result
    assert Protocol.response_type_from_wire("ok") == :ok
    assert Protocol.response_type_from_wire("bogus") == :unknown
  end

  test "single-key fencing token is preserved exactly and fails closed" do
    max_exact = 9_007_199_254_740_991
    assert Protocol.fencing_token_from_response(%{"fencingToken" => max_exact}) == {:ok, max_exact}
    assert Protocol.fencing_token_from_response(%{fencingToken: 42}) == {:ok, 42}
    assert Protocol.fencing_token_from_response(%{}) == {:error, :missing_fencing_token}
    assert Protocol.fencing_token_from_response(%{"fencingToken" => 0}) == {:error, :invalid_fencing_token}
    assert Protocol.fencing_token_from_response(%{"fencingToken" => 1.5}) == {:error, :invalid_fencing_token}
  end

  test "composite fencing tokens remain scoped to their keys" do
    tokens = %{"a" => 5, "b" => 12}
    assert Protocol.fencing_tokens_from_response(%{"fencingTokens" => tokens}) == {:ok, tokens}
    assert Protocol.fencing_tokens_from_response(%{"fencingTokens" => %{"a" => 0}}) == {:error, :invalid_fencing_tokens}
    assert Protocol.fencing_tokens_from_response(%{}) == {:error, :missing_fencing_tokens}
  end
end

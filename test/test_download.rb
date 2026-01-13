require "common"

class DownloadTest < Net::SFTP::TestCase
  FXP_DATA_CHUNK_SIZE = 1024

  def setup
    prepare_progress!
    @original_file_open = File.method(:open)
  end

  def teardown
    verbose, $VERBOSE = $VERBOSE, nil
    File.define_singleton_method(:open, @original_file_open)
  ensure
    $VERBOSE = verbose
  end

  def test_download_file_should_transfer_remote_to_local
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "this is some text\n"

    expect_file_transfer(remote, text)

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local) }
    assert_equal text, file.string
  end

  def test_download_file_should_transfer_remote_to_local_in_spite_of_fragmentation
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "this is some text\n"

    expect_file_transfer(remote, text, :fragment_len => 1)

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local) }
    assert_equal text, file.string
  end

  def test_download_large_file_should_transfer_remote_to_local
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "0123456789" * 1024

    file = prepare_large_file_download(local, remote, text)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1024) }
    assert_equal text, file.string
  end

  def test_download_large_file_should_handle_too_large_read_size
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "0123456789" * 1024

    # some servers put upper bound on the max read_size value and send less data than requested
    too_large_read_size = FXP_DATA_CHUNK_SIZE + 1
    file = prepare_large_file_download(local, remote, text, too_large_read_size)

    assert_scripted_command { sftp.download(remote, local, :read_size => too_large_read_size) }
    assert_equal text, file.string
  end

  def test_download_large_file_with_progress_should_report_progress
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "0123456789" * 1024

    file = prepare_large_file_download(local, remote, text)

    assert_scripted_command do
      sftp.download(remote, local, :read_size => 1024) do |*args|
        record_progress(args)
      end
    end

    assert_equal text, file.string

    assert_progress_reported_open :remote => "/path/to/remote"
    assert_progress_reported_get     0, 1024
    assert_progress_reported_get  1024, 1024
    assert_progress_reported_get  2048, 1024
    assert_progress_reported_get  3072, 1024
    assert_progress_reported_get  4096, 1024
    assert_progress_reported_get  5120, 1024
    assert_progress_reported_get  6144, 1024
    assert_progress_reported_get  7168, 1024
    assert_progress_reported_get  8192, 1024
    assert_progress_reported_get  9216, 1024
    assert_progress_reported_close
    assert_progress_reported_finish
    assert_no_more_reported_events
  end

  def test_download_directory_should_mirror_directory_locally
    file1, file2 = prepare_directory_tree_download("/path/to/local", "/path/to/remote", :requests => 2)

    assert_scripted_command do
      sftp.download("/path/to/remote", "/path/to/local", :recursive => true, :requests => 2)
    end

    assert_equal "contents of file1", file1.string
    assert_equal "contents of file2", file2.string
  end

  def test_download_directory_with_progress_should_report_progress
    file1, file2 = prepare_directory_tree_download("/path/to/local", "/path/to/remote", :requests => 2)

    assert_scripted_command do
      sftp.download("/path/to/remote", "/path/to/local", :recursive => true, :requests => 2) do |*args|
        record_progress(args)
      end
    end

    assert_equal "contents of file1", file1.string
    assert_equal "contents of file2", file2.string

    # Progress events happen in this order with pipelining:
    # - mkdirs happen when entries are processed from the stack
    # - file2 opens before file1 data arrives (due to packet ordering)
    # - file1 closes before file2 data arrives (due to pipelined reads completing)
    assert_progress_reported_mkdir "/path/to/local"
    assert_progress_reported_mkdir "/path/to/local/subdir1"
    assert_progress_reported_open  :remote => "/path/to/remote/file1"
    assert_progress_reported_open  :remote => "/path/to/remote/subdir1/file2"
    assert_progress_reported_get   0, "contents of file1"
    assert_progress_reported_close :remote => "/path/to/remote/file1"
    assert_progress_reported_get   0, "contents of file2"
    assert_progress_reported_close :remote => "/path/to/remote/subdir1/file2"
    assert_progress_reported_finish
    assert_no_more_reported_events
  end

  def test_download_directory_with_pipelining_should_send_multiple_reads
    file1, file2 = prepare_directory_tree_download_with_pipelining("/path/to/local", "/path/to/remote", :requests => 2, :pipeline => 2)

    assert_scripted_command do
      sftp.download("/path/to/remote", "/path/to/local", :recursive => true, :requests => 2, :pipeline => 2)
    end

    assert_equal "contents of file1", file1.string
    assert_equal "contents of file2", file2.string
  end

  def test_download_file_should_transfer_remote_to_local_buffer
    remote = "/path/to/remote"
    text = "this is some text\n"

    expect_file_transfer(remote, text)

    local = StringIO.new

    assert_scripted_command { sftp.download(remote, local) }
    assert_equal text, local.string
  end

  def test_download_directory_to_buffer_should_fail
    expect_sftp_session :server_version => 3
    Net::SSH::Test::Extensions::IO.with_test_extension do
      assert_raises(ArgumentError) { sftp.download("/path/to/remote", StringIO.new, :recursive => true) }
    end
  end

  # Pipelining tests - verify multiple concurrent read requests

  def test_download_with_pipelining_sends_multiple_requests_upfront
    local = "/path/to/local"
    remote = "/path/to/remote"
    chunk1 = "A" * 1000
    chunk2 = "B" * 1000
    text = chunk1 + chunk2

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Pipelining sends 2 reads upfront before any response
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1000)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1000, :long, 1000)

      # First response, triggers another read to refill pipeline
      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 2000, :long, 1000)

      # Second response, triggers another read
      channel.gets_packet(FXP_DATA, :long, 2, :string, chunk2)
      channel.sends_packet(FXP_READ, :long, 4, :string, "handle", :int64, 3000, :long, 1000)

      # EOF responses
      channel.gets_packet(FXP_STATUS, :long, 3, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 4, :long, 1)

      channel.sends_packet(FXP_CLOSE, :long, 5, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 5, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1000, :pipeline => 2) }
    assert_equal text, file.string
  end

  def test_download_with_pipelining_handles_out_of_order_responses
    local = "/path/to/local"
    remote = "/path/to/remote"
    chunk1 = "AAAA" * 256  # 1024 bytes at offset 0
    chunk2 = "BBBB" * 256  # 1024 bytes at offset 1024
    chunk3 = "CCCC" * 256  # 1024 bytes at offset 2048
    text = chunk1 + chunk2 + chunk3

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Send 3 requests upfront
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1024, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 2048, :long, 1024)

      # Responses come back OUT OF ORDER: 2, 3, 1
      channel.gets_packet(FXP_DATA, :long, 2, :string, chunk2)
      channel.sends_packet(FXP_READ, :long, 4, :string, "handle", :int64, 3072, :long, 1024)

      channel.gets_packet(FXP_DATA, :long, 3, :string, chunk3)
      channel.sends_packet(FXP_READ, :long, 5, :string, "handle", :int64, 4096, :long, 1024)

      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 6, :string, "handle", :int64, 5120, :long, 1024)

      # EOF responses
      channel.gets_packet(FXP_STATUS, :long, 4, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 5, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 6, :long, 1)

      channel.sends_packet(FXP_CLOSE, :long, 7, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 7, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1024, :pipeline => 3) }
    assert_equal text, file.string
  end

  def test_download_with_pipelining_waits_for_all_pending_requests_before_close
    local = "/path/to/local"
    remote = "/path/to/remote"
    chunk1 = "X" * 512
    chunk2 = "Y" * 512
    text = chunk1 + chunk2

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Send 4 requests upfront for a small file (will get lots of EOFs)
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 512)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 512, :long, 512)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 1024, :long, 512)
      channel.sends_packet(FXP_READ, :long, 4, :string, "handle", :int64, 1536, :long, 512)

      # Data responses refill pipeline
      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 5, :string, "handle", :int64, 2048, :long, 512)

      channel.gets_packet(FXP_DATA, :long, 2, :string, chunk2)
      channel.sends_packet(FXP_READ, :long, 6, :string, "handle", :int64, 2560, :long, 512)

      # EOF for requests 3-6 (requests past end of file)
      channel.gets_packet(FXP_STATUS, :long, 3, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 4, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 5, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 6, :long, 1)

      channel.sends_packet(FXP_CLOSE, :long, 7, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 7, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 512, :pipeline => 4) }
    assert_equal text, file.string
  end

  def test_download_with_default_pipeline_uses_sequential_reads
    local = "/path/to/local"
    remote = "/path/to/remote"
    text = "sequential download test"

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # With default pipeline=1, only one READ at a time
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 32_000)
      channel.gets_packet(FXP_DATA, :long, 1, :string, text)

      # With pipeline=1 and short read detection, next read is at text.bytesize
      # (This correctly handles servers that cap read size)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, text.bytesize, :long, 32_000)
      channel.gets_packet(FXP_STATUS, :long, 2, :long, 1)  # EOF

      channel.sends_packet(FXP_CLOSE, :long, 3, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 3, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local) }
    assert_equal text, file.string
  end

  def test_download_pipelining_handles_partial_final_chunk
    local = "/path/to/local"
    remote = "/path/to/remote"
    chunk1 = "A" * 1024
    chunk2 = "B" * 476
    text = chunk1 + chunk2

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Pipeline sends 2 requests upfront
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1024, :long, 1024)

      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 2048, :long, 1024)

      # Second response is partial (only 476 bytes) - triggers gap fill at 1500
      # Gap size is 1024 - 476 = 548 bytes
      channel.gets_packet(FXP_DATA, :long, 2, :string, chunk2)
      channel.sends_packet(FXP_READ, :long, 4, :string, "handle", :int64, 1500, :long, 548)   # gap fill (exactly 548)
      channel.sends_packet(FXP_READ, :long, 5, :string, "handle", :int64, 3072, :long, 1024)  # pipeline refill

      channel.gets_packet(FXP_STATUS, :long, 3, :long, 1)  # EOF
      channel.gets_packet(FXP_STATUS, :long, 4, :long, 1)  # EOF (gap fill past end)
      channel.gets_packet(FXP_STATUS, :long, 5, :long, 1)  # EOF

      channel.sends_packet(FXP_CLOSE, :long, 6, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 6, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1024, :pipeline => 2) }
    assert_equal text, file.string
  end

  def test_download_with_pipelining_fills_gaps_when_server_caps_read_size
    local = "/path/to/local"
    remote = "/path/to/remote"
    # Server caps reads to 512 bytes, but we request 1024
    chunk1 = "A" * 512  # offset 0-511 (short read, gap at 512-1023)
    chunk2 = "B" * 512  # offset 1024-1535 (short read, gap at 1536-2047)
    chunk3 = "C" * 512  # gap fill at 512-1023, exactly fills the gap
    chunk4 = "D" * 512  # gap fill at 1536-2047, exactly fills the gap
    text = chunk1 + chunk3 + chunk2 + chunk4

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Pipeline sends 2 requests upfront
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1024, :long, 1024)

      # First response is short (512 bytes) - triggers gap fill of 512 bytes
      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 512, :long, 512)    # gap fill (exactly 512)
      channel.sends_packet(FXP_READ, :long, 4, :string, "handle", :int64, 2048, :long, 1024)  # pipeline refill

      # Second response is short (512 bytes) - triggers gap fill of 512 bytes
      channel.gets_packet(FXP_DATA, :long, 2, :string, chunk2)
      channel.sends_packet(FXP_READ, :long, 5, :string, "handle", :int64, 1536, :long, 512)   # gap fill (exactly 512)
      channel.sends_packet(FXP_READ, :long, 6, :string, "handle", :int64, 3072, :long, 1024)  # pipeline refill

      # Gap fill at 512 returns 512 bytes - fills gap exactly, no further gap fill needed
      channel.gets_packet(FXP_DATA, :long, 3, :string, chunk3)
      channel.sends_packet(FXP_READ, :long, 7, :string, "handle", :int64, 4096, :long, 1024)  # pipeline refill

      # Pipeline refill at 2048 returns EOF - sets eof_seen=true
      channel.gets_packet(FXP_STATUS, :long, 4, :long, 1)

      # Gap fill at 1536 returns 512 bytes - fills gap exactly
      # Since eof_seen=true, no further pipeline refill is sent
      channel.gets_packet(FXP_DATA, :long, 5, :string, chunk4)

      # Remaining reads return EOF
      channel.gets_packet(FXP_STATUS, :long, 6, :long, 1)
      channel.gets_packet(FXP_STATUS, :long, 7, :long, 1)

      channel.sends_packet(FXP_CLOSE, :long, 8, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 8, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1024, :pipeline => 2) }
    assert_equal text, file.string
  end

  def test_download_with_pipelining_handles_empty_file
    local = "/path/to/local"
    remote = "/path/to/remote"

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Pipeline sends 2 requests upfront
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1024, :long, 1024)

      # Both requests immediately return EOF
      channel.gets_packet(FXP_STATUS, :long, 1, :long, 1)  # EOF
      channel.gets_packet(FXP_STATUS, :long, 2, :long, 1)  # EOF

      channel.sends_packet(FXP_CLOSE, :long, 3, :string, "handle")
      channel.gets_packet(FXP_STATUS, :long, 3, :long, 0)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_scripted_command { sftp.download(remote, local, :read_size => 1024, :pipeline => 2) }
    assert_equal "", file.string
  end

  def test_download_with_pipelining_raises_on_server_error
    local = "/path/to/local"
    remote = "/path/to/remote"
    chunk1 = "A" * 1024

    expect_sftp_session :server_version => 3 do |channel|
      channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
      channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")

      # Pipeline sends 2 requests upfront
      channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, 1024)
      channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, 1024, :long, 1024)

      # First request succeeds
      channel.gets_packet(FXP_DATA, :long, 1, :string, chunk1)
      channel.sends_packet(FXP_READ, :long, 3, :string, "handle", :int64, 2048, :long, 1024)

      # Second request returns error (e.g., permission denied = status 3)
      channel.gets_packet(FXP_STATUS, :long, 2, :long, 3)
    end

    file = StringIO.new
    File.stubs(:open).with(local, "wb").returns(file)

    assert_raises(Net::SFTP::StatusException) do
      assert_scripted_command { sftp.download(remote, local, :read_size => 1024, :pipeline => 2) }
    end
  end

  private

    def expect_file_transfer(remote, text, opts={})
      read_size = 32_000
      expect_sftp_session :server_version => 3 do |channel|
        channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
        channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")
        channel.sends_packet(FXP_READ, :long, 1, :string, "handle", :int64, 0, :long, read_size)
        channel.gets_packet_in_two(opts[:fragment_len], FXP_DATA, :long, 1, :string, text)
        # After short read detection, next offset is based on actual bytes received
        channel.sends_packet(FXP_READ, :long, 2, :string, "handle", :int64, text.bytesize, :long, read_size)
        channel.gets_packet(FXP_STATUS, :long, 2, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 3, :string, "handle")
        channel.gets_packet(FXP_STATUS, :long, 3, :long, 0)
      end
    end

    def prepare_large_file_download(local, remote, text, requested_chunk_size = FXP_DATA_CHUNK_SIZE)
      expect_sftp_session :server_version => 3 do |channel|
        channel.sends_packet(FXP_OPEN, :long, 0, :string, remote, :long, 0x01, :long, 0)
        channel.gets_packet(FXP_HANDLE, :long, 0, :string, "handle")
        data_packet_count = (text.bytesize / FXP_DATA_CHUNK_SIZE.to_f).ceil
        data_packet_count.times do |n|
          payload = text[n*FXP_DATA_CHUNK_SIZE,FXP_DATA_CHUNK_SIZE]
          actual_offset = n * FXP_DATA_CHUNK_SIZE
          # After 3 consecutive short reads, read_size adapts to server's cap
          effective_chunk_size = if n >= 3 && requested_chunk_size > FXP_DATA_CHUNK_SIZE
            FXP_DATA_CHUNK_SIZE
          else
            requested_chunk_size
          end
          channel.sends_packet(FXP_READ, :long, n+1, :string, "handle", :int64, actual_offset, :long, effective_chunk_size)
          channel.gets_packet(FXP_DATA, :long, n+1, :string, payload)
        end
        # Final read uses adapted size if applicable
        final_chunk_size = if data_packet_count >= 3 && requested_chunk_size > FXP_DATA_CHUNK_SIZE
          FXP_DATA_CHUNK_SIZE
        else
          requested_chunk_size
        end
        channel.sends_packet(FXP_READ, :long, data_packet_count + 1, :string, "handle", :int64, data_packet_count * FXP_DATA_CHUNK_SIZE, :long, final_chunk_size)
        channel.gets_packet(FXP_STATUS, :long, data_packet_count + 1, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, data_packet_count + 2, :string, "handle")
        channel.gets_packet(FXP_STATUS, :long, data_packet_count + 2, :long, 0)
      end

      file = StringIO.new
      original_file_open = @original_file_open
      verbose, $VERBOSE = $VERBOSE, nil
      File.define_singleton_method(:open) do |*args, **kwargs, &block|
        if args == [local, "wb"] && kwargs.empty?
          file
        else
          original_file_open.call(*args, **kwargs, &block)
        end
      end
      $VERBOSE = verbose

      return file
    end

    # 0:OPENDIR(remote) ->
    # <- 0:HANDLE("dir1")
    # 1:READDIR("dir1") ->
    # <- 1:NAME("..", ".", "subdir1", "file1")
    # 2:OPENDIR(remote/subdir1) ->
    # 3:OPEN(remote/file1) ->
    # 4:READDIR("dir1") ->
    # <- 2:HANDLE("dir2")
    # 5:READDIR("dir2") ->
    # <- 3:HANDLE("file1")
    # 6:READ("file1", 0, 32k) ->
    # <- 4:STATUS(1)
    # 7:CLOSE("dir1") ->
    # <- 5:NAME("..", ".", "file2")
    # 8:OPEN(remote/subdir1/file2) ->
    # 9:READDIR("dir2") ->
    # <- 6:DATA("blah blah blah")
    # 10:READ("file1", n, 32k)
    # <- 7:STATUS(0)
    # <- 8:HANDLE("file2")
    # 11:READ("file2", 0, 32k) ->
    # <- 9:STATUS(1)
    # 12:CLOSE("dir2") ->
    # <- 10:STATUS(1)
    # 13:CLOSE("file1") ->
    # <- 11:DATA("blah blah blah")
    # 14:READ("file2", n, 32k) ->
    # <- 12:STATUS(0)
    # <- 13:STATUS(0)
    # <- 14:STATUS(1)
    # 15:CLOSE("file2") ->
    # <- 15:STATUS(0)

    # Prepares a directory tree download test.
    # requests controls file/directory concurrency, pipeline defaults to 1 (sequential).
    def prepare_directory_tree_download(local, remote, opts = {})
      file1_contents = "contents of file1"
      file2_contents = "contents of file2"
      read_size = 32_000

      expect_sftp_session :server_version => 3 do |channel|
        # 0: Open the root directory (active=1)
        channel.sends_packet(FXP_OPENDIR, :long, 0, :string, remote)
        channel.gets_packet(FXP_HANDLE, :long, 0, :string, "dir1")

        # 1: Read root directory contents
        channel.sends_packet(FXP_READDIR, :long, 1, :string, "dir1")
        channel.gets_packet(FXP_NAME, :long, 1, :long, 4,
          :string, "..",      :string, "drwxr-xr-x  4 bob bob  136 Aug  1 ..", :long, 0x04, :long, 040755,
          :string, ".",       :string, "drwxr-xr-x  4 bob bob  136 Aug  1 .", :long, 0x04, :long, 040755,
          :string, "subdir1", :string, "drwxr-xr-x  4 bob bob  136 Aug  1 subdir1", :long, 0x04, :long, 040755,
          :string, "file1",   :string, "-rw-rw-r--  1 bob bob  100 Aug  1 file1", :long, 0x04, :long, 0100644)

        # on_readdir adds subdir1, file1 to stack
        # process_next_entry: requests(2) > active(1), start subdir1 (active=2)
        channel.sends_packet(FXP_OPENDIR, :long, 2, :string, File.join(remote, "subdir1"))
        channel.sends_packet(FXP_READDIR, :long, 3, :string, "dir1")

        # Get handle for subdir1
        channel.gets_packet(FXP_HANDLE, :long, 2, :string, "dir2")
        channel.sends_packet(FXP_READDIR, :long, 4, :string, "dir2")

        # dir1 readdir returns EOF
        channel.gets_packet(FXP_STATUS, :long, 3, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 5, :string, "dir1")

        # dir2 returns file2
        channel.gets_packet(FXP_NAME, :long, 4, :long, 3,
          :string, "..",    :string, "drwxr-xr-x  4 bob bob  136 Aug  1 ..", :long, 0x04, :long, 040755,
          :string, ".",     :string, "drwxr-xr-x  4 bob bob  136 Aug  1 .", :long, 0x04, :long, 040755,
          :string, "file2", :string, "-rw-rw-r--  1 bob bob  100 Aug  1 file2", :long, 0x04, :long, 0100644)

        channel.sends_packet(FXP_READDIR, :long, 6, :string, "dir2")

        # Close dir1 succeeds, process_next_entry starts file1
        channel.gets_packet(FXP_STATUS, :long, 5, :long, 0)
        channel.sends_packet(FXP_OPEN, :long, 7, :string, File.join(remote, "file1"), :long, 0x01, :long, 0)

        # dir2 readdir returns EOF
        channel.gets_packet(FXP_STATUS, :long, 6, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 8, :string, "dir2")

        # Get file1 handle, send one read (pipeline=1)
        channel.gets_packet(FXP_HANDLE, :long, 7, :string, "file1")
        channel.sends_packet(FXP_READ, :long, 9, :string, "file1", :int64, 0, :long, read_size)

        # Close dir2 succeeds, process_next_entry starts file2
        channel.gets_packet(FXP_STATUS, :long, 8, :long, 0)
        channel.sends_packet(FXP_OPEN, :long, 10, :string, File.join(remote, "subdir1", "file2"), :long, 0x01, :long, 0)

        # file1 read returns data (short read, 17 bytes < 32000)
        # With pipeline=1, short read adjusts next_offset
        channel.gets_packet(FXP_DATA, :long, 9, :string, file1_contents)
        channel.sends_packet(FXP_READ, :long, 11, :string, "file1", :int64, file1_contents.bytesize, :long, read_size)

        # Get file2 handle, send one read
        channel.gets_packet(FXP_HANDLE, :long, 10, :string, "file2")
        channel.sends_packet(FXP_READ, :long, 12, :string, "file2", :int64, 0, :long, read_size)

        # file1 second read returns EOF, close file
        channel.gets_packet(FXP_STATUS, :long, 11, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 13, :string, "file1")

        # file2 read returns data
        channel.gets_packet(FXP_DATA, :long, 12, :string, file2_contents)
        channel.sends_packet(FXP_READ, :long, 14, :string, "file2", :int64, file2_contents.bytesize, :long, read_size)

        # file1 close succeeds
        channel.gets_packet(FXP_STATUS, :long, 13, :long, 0)

        # file2 second read returns EOF, close file
        channel.gets_packet(FXP_STATUS, :long, 14, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 15, :string, "file2")
        channel.gets_packet(FXP_STATUS, :long, 15, :long, 0)
      end

      File.expects(:directory?).with(local).returns(false)
      File.expects(:directory?).with(File.join(local, "subdir1")).returns(false)
      Dir.expects(:mkdir).with(local)
      Dir.expects(:mkdir).with(File.join(local, "subdir1"))

      file1 = StringIO.new
      file2 = StringIO.new
      File.expects(:open).with(File.join(local, "file1"), "wb").returns(file1)
      File.expects(:open).with(File.join(local, "subdir1", "file2"), "wb").returns(file2)

      [file1, file2]
    end

    # Prepares a directory tree download test with pipelining enabled (pipeline > 1).
    # This sends multiple read requests per file upfront.
    def prepare_directory_tree_download_with_pipelining(local, remote, opts = {})
      file1_contents = "contents of file1"
      file2_contents = "contents of file2"
      read_size = 32_000

      expect_sftp_session :server_version => 3 do |channel|
        # 0: Open the root directory (active=1)
        channel.sends_packet(FXP_OPENDIR, :long, 0, :string, remote)
        channel.gets_packet(FXP_HANDLE, :long, 0, :string, "dir1")

        # 1: Read root directory contents
        channel.sends_packet(FXP_READDIR, :long, 1, :string, "dir1")
        channel.gets_packet(FXP_NAME, :long, 1, :long, 4,
          :string, "..",      :string, "drwxr-xr-x  4 bob bob  136 Aug  1 ..", :long, 0x04, :long, 040755,
          :string, ".",       :string, "drwxr-xr-x  4 bob bob  136 Aug  1 .", :long, 0x04, :long, 040755,
          :string, "subdir1", :string, "drwxr-xr-x  4 bob bob  136 Aug  1 subdir1", :long, 0x04, :long, 040755,
          :string, "file1",   :string, "-rw-rw-r--  1 bob bob  100 Aug  1 file1", :long, 0x04, :long, 0100644)

        channel.sends_packet(FXP_OPENDIR, :long, 2, :string, File.join(remote, "subdir1"))
        channel.sends_packet(FXP_READDIR, :long, 3, :string, "dir1")

        channel.gets_packet(FXP_HANDLE, :long, 2, :string, "dir2")
        channel.sends_packet(FXP_READDIR, :long, 4, :string, "dir2")

        channel.gets_packet(FXP_STATUS, :long, 3, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 5, :string, "dir1")

        channel.gets_packet(FXP_NAME, :long, 4, :long, 3,
          :string, "..",    :string, "drwxr-xr-x  4 bob bob  136 Aug  1 ..", :long, 0x04, :long, 040755,
          :string, ".",     :string, "drwxr-xr-x  4 bob bob  136 Aug  1 .", :long, 0x04, :long, 040755,
          :string, "file2", :string, "-rw-rw-r--  1 bob bob  100 Aug  1 file2", :long, 0x04, :long, 0100644)

        channel.sends_packet(FXP_READDIR, :long, 6, :string, "dir2")

        channel.gets_packet(FXP_STATUS, :long, 5, :long, 0)
        channel.sends_packet(FXP_OPEN, :long, 7, :string, File.join(remote, "file1"), :long, 0x01, :long, 0)

        channel.gets_packet(FXP_STATUS, :long, 6, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 8, :string, "dir2")

        # Get file1 handle, send pipelined reads (pipeline=2)
        channel.gets_packet(FXP_HANDLE, :long, 7, :string, "file1")
        channel.sends_packet(FXP_READ, :long, 9, :string, "file1", :int64, 0, :long, read_size)
        channel.sends_packet(FXP_READ, :long, 10, :string, "file1", :int64, read_size, :long, read_size)

        channel.gets_packet(FXP_STATUS, :long, 8, :long, 0)
        channel.sends_packet(FXP_OPEN, :long, 11, :string, File.join(remote, "subdir1", "file2"), :long, 0x01, :long, 0)

        # file1 first read returns data (short read), triggers gap fill and pipeline refill
        channel.gets_packet(FXP_DATA, :long, 9, :string, file1_contents)
        channel.sends_packet(FXP_READ, :long, 12, :string, "file1", :int64, file1_contents.bytesize, :long, read_size - file1_contents.bytesize)  # gap fill
        channel.sends_packet(FXP_READ, :long, 13, :string, "file1", :int64, read_size * 2, :long, read_size)  # pipeline refill

        # file1 second read returns EOF
        channel.gets_packet(FXP_STATUS, :long, 10, :long, 1)

        # Get file2 handle, send pipelined reads
        channel.gets_packet(FXP_HANDLE, :long, 11, :string, "file2")
        channel.sends_packet(FXP_READ, :long, 14, :string, "file2", :int64, 0, :long, read_size)
        channel.sends_packet(FXP_READ, :long, 15, :string, "file2", :int64, read_size, :long, read_size)

        # file1 gap-fill and third read return EOF, close file
        channel.gets_packet(FXP_STATUS, :long, 12, :long, 1)
        channel.gets_packet(FXP_STATUS, :long, 13, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 16, :string, "file1")

        # file2 first read returns data (short read), triggers gap fill and pipeline refill
        channel.gets_packet(FXP_DATA, :long, 14, :string, file2_contents)
        channel.sends_packet(FXP_READ, :long, 17, :string, "file2", :int64, file2_contents.bytesize, :long, read_size - file2_contents.bytesize)  # gap fill
        channel.sends_packet(FXP_READ, :long, 18, :string, "file2", :int64, read_size * 2, :long, read_size)  # pipeline refill

        # file2 second read returns EOF
        channel.gets_packet(FXP_STATUS, :long, 15, :long, 1)

        # Close file1 succeeds
        channel.gets_packet(FXP_STATUS, :long, 16, :long, 0)

        # file2 gap-fill and third read return EOF, close file
        channel.gets_packet(FXP_STATUS, :long, 17, :long, 1)
        channel.gets_packet(FXP_STATUS, :long, 18, :long, 1)
        channel.sends_packet(FXP_CLOSE, :long, 19, :string, "file2")
        channel.gets_packet(FXP_STATUS, :long, 19, :long, 0)
      end

      File.expects(:directory?).with(local).returns(false)
      File.expects(:directory?).with(File.join(local, "subdir1")).returns(false)
      Dir.expects(:mkdir).with(local)
      Dir.expects(:mkdir).with(File.join(local, "subdir1"))

      file1 = StringIO.new
      file2 = StringIO.new
      File.expects(:open).with(File.join(local, "file1"), "wb").returns(file1)
      File.expects(:open).with(File.join(local, "subdir1", "file2"), "wb").returns(file2)

      [file1, file2]
    end
end
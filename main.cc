#include <seastar/core/app-template.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>
#include <seastar/websocket/server.hh>

#include <exception>
#include <iostream>

namespace ss = seastar;
namespace ws = seastar::experimental::websocket;
namespace po = boost::program_options;
using stop_iter = ss::stop_iteration;
using char_buf = ss::temporary_buffer<char>;

static ss::logger lg{"wsrp"};

ss::future<> handler(
  ss::shared_ptr<ss::queue<ss::sstring>>& q,
  ss::input_stream<char>& in,
  ss::output_stream<char>& out) {
    return ss::repeat([&in, &q] {
        // Early abort: if our input stream is already closed, give up.
        if (in.eof()) return ss::make_ready_future<stop_iter>(stop_iter::yes);

        return in
          // Read our 2-byte length of the key.
          .read_exactly(2)
          .then([&in](char_buf buf) {
              if (buf.size() != 2 || in.eof())
                  return ss::make_exception_future<char_buf>(
                    std::runtime_error("disconnect"));
              size_t len = ss::read_be<uint16_t>(buf.get());
              return in.read_exactly(len);
          })
          // Read our key.
          .then([&in](char_buf buf) {
              if (buf.size() == 0 || in.eof())
                  return ss::make_exception_future<char_buf>(
                    std::runtime_error("disconnect"));
              auto key = ss::sstring(buf.get(), buf.size());
              lg.info("key: {}", key);
              return in.read_exactly(2);
          })
          // Read the 2-byte length of the value.
          .then([&in](char_buf buf) {
              if (buf.size() != 2 || in.eof())
                  return ss::make_exception_future<char_buf>(
                    std::runtime_error("disconnect"));
              size_t len = ss::read_be<uint16_t>(buf.get());
              return in.read_exactly(len);
          })
          // Read the value.
          .then([&in, &q](char_buf buf) {
              if (buf.size() == 0 || in.eof())
                  return ss::make_exception_future<stop_iter>(
                    std::runtime_error("disconnect"));
              auto val = ss::sstring(buf.get(), buf.size());
              return q->push_eventually(std::move(val)).then([] {
                  return ss::make_ready_future<stop_iter>(stop_iter::no);
              });
          })
          // If anything goes wrong, consider it a disconnect for now.
          .handle_exception([](std::exception_ptr e) {
              try {
                  std::rethrow_exception(e);
              } catch (const std::exception& e) {
                  lg.info("{}", e.what());
              }
              return ss::make_ready_future<stop_iter>(stop_iter::yes);
          });
    });
}

class wsrp_service {
    ss::socket_address _sa;
    std::optional<ws::server> _ws;
    ss::shared_ptr<ss::queue<ss::sstring>> _queue;

public:
    wsrp_service(ss::socket_address sa)
      : _sa(sa)
      , _queue(ss::make_shared<ss::queue<ss::sstring>>(256)) {
        _ws = ws::server{};
    }

    ss::future<> run() {
        try {
            // Create a handler function bound to our shard-local queue.
            auto fn = std::bind(
              handler, _queue, std::placeholders::_1, std::placeholders::_2);
            _ws->register_handler("dumb-ws", fn);
            _ws->listen(_sa, {true});
        } catch (std::system_error& e) {
            lg.error("failed to listen on {}: {}", _sa, e);
            return ss::make_exception_future(std::move(e));
        } catch (std::exception& e) {
            lg.error("uh oh! {}", e);
            return ss::make_exception_future(std::move(e));
        }

        lg.info("listening on {}", _sa);
        return ss::keep_doing([this] {
                   return _queue->pop_eventually().then([](ss::sstring val) {
                       lg.info("popped {}", val);
                       return ss::make_ready_future<>();
                   });
               })
          .handle_exception([](auto unused) {
              lg.debug("done popping");
              return ss::make_ready_future<>();
          });
    }

    ss::future<> stop() {
        lg.info("stopping...");
        if (_ws) return _ws->stop();
        return ss::make_ready_future<>();
    }
};

int main(int argc, char** argv) {
    ss::app_template app;
    ss::sharded<wsrp_service> s;

    auto opts = app.add_options();
    opts(
      "address",
      po::value<std::string>()->default_value("127.0.0.1"),
      "listen address");
    opts(
      "port", po::value<std::uint16_t>()->default_value(8088), "listen port");
    opts(
      "timeout",
      po::value<std::int32_t>()->default_value(-1),
      "number of seconds to listen for (-1 for infinite)");

    return app.run(argc, argv, [&] {
        auto& config = app.configuration();
        auto& addr = config["address"].as<std::string>();
        auto& port = config["port"].as<std::uint16_t>();
        auto& timeout = config["timeout"].as<std::int32_t>();
        auto sa = ss::socket_address(ss::ipv4_addr(addr, port));

        return s.start(sa)
          .then([&] {
              return s
                // Start our service on all shards.
                .invoke_on_all(
                  [](wsrp_service& local_s) { (void)local_s.run(); })
                // Queue up a sleeping fiber so we can stop.
                .then([&timeout] {
                    auto dur = (timeout > 0) ? std::chrono::seconds(timeout)
                                             : std::chrono::years(100);
                    return ss::sleep_abortable(dur).handle_exception(
                      [](auto ignored) {});
                });
          })
          .then([&] { return s.stop(); });
    });
}

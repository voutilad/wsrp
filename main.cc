#include <seastar/core/app-template.hh>
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

static ss::logger lg("wsrp");

class wsrp_service {
    ss::socket_address _sa;
    std::optional<ws::server> _ws;

    // Static for now as w don't need any service state.

    static ss::future<>
    handler(ss::input_stream<char>& in, ss::output_stream<char>& out) {
        return ss::repeat([&in, &out] {
            return in.read().then([&out](ss::temporary_buffer<char> buf) {
                // Check for disconnect.
                if (buf.empty()) {
                    return make_ready_future<ss::stop_iteration>(
                      ss::stop_iteration::yes);
                }

                lg.info("got {} bytes", buf.size());
                return out.write(std::move(buf)).then([&out] {
                    return out.flush().then([] {
                        return make_ready_future<ss::stop_iteration>(
                          ss::stop_iteration::no);
                    });
                });
            });
        });
    }

public:
    wsrp_service(ss::socket_address sa)
      : _sa(sa) {
        _ws = ws::server{};
    }

    ss::future<> run() {
        try {
	    _ws->register_handler("dumb-ws", handler);
            _ws->listen(_sa, {true});
        } catch (std::system_error& e) {
            lg.error("failed to listen on {}: {}", _sa, e);
            return ss::make_exception_future(std::move(e));
        } catch (std::exception& e) {
            lg.error("uh oh! {}", e);
            return ss::make_exception_future(std::move(e));
        }

        lg.info("listening on {}", _sa);
        return ss::make_ready_future<>();
    }

    ss::future<> stop() {
        lg.info("stopping...");
        // if (_sock) _sock->abort_accept();
        if (_ws) return _ws->stop();
        return ss::make_ready_future<>();
    }
};

// Put it in the global scope so it outlives most things.
ss::sharded<wsrp_service> s;

int main(int argc, char** argv) {
    ss::app_template app;
    auto sa = ss::socket_address(ss::ipv4_addr("127.0.0.1", 8088));

    return app.run(argc, argv, [&] {
        return s.start(sa)
          .then([] {
              return s
                // Start our service on all shards.
                .invoke_on_all(
                  [](wsrp_service& local_s) { (void)local_s.run(); })
                // Queue up a sleeping fiber so we can stop.
                .then([] {
                    return ss::sleep_abortable(std::chrono::seconds(300))
                      .handle_exception([](auto ignored) {});
                });
          })
          .then([] { return s.stop(); });
    });
}

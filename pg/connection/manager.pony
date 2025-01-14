use "collections"
use "promises"
use "logger"

use "pg"

actor ConnectionManager
  let _connections: Array[BEConnection tag] = Array[BEConnection tag]
  let _params: Array[(String, String)] val
  let _host: String
  let _service: String
  let _user: String
  let _passwd_provider: PasswordProvider tag
  var _password: (String | None) = None
  let _max_size: USize
  let out: OutStream
  let logger: Logger[String val] val

  new create(host: String,
             service: String,
             user: String,
             passwd_provider: PasswordProvider tag,
             params: Array[Param] val,
             out': OutStream,
             pool_size: USize = 1
             ) =>
    _params = params
    _host = host
    _service = service
    _passwd_provider = passwd_provider
    _user = user
    _max_size = pool_size
    out = out'
    logger = StringLogger(Fine, out)

  be log(msg: String) =>
    None

  be connect(auth: AmbientAuth, f: ({(Connection tag):(Any)} val | Promise[Connection tag] val)) =>
    let priv_conn=_Connection(auth, _host, _service, _params, this, out)
    _connections.push(priv_conn)
    let conn = Connection(priv_conn)
    priv_conn.set_frontend(conn)
    match recover val f end
    | let f': Promise[Connection tag] => f'(conn)
    | let f': {(Connection tag):(Any)} val => f'(conn)
    end

  be connect_p(auth: AmbientAuth, f: {(Connection tag)} iso) =>
    let priv_conn=_Connection(auth, _host, _service, _params, this, out)
    _connections.push(priv_conn)
    let conn = Connection(priv_conn)
    priv_conn.set_frontend(conn)
    f(conn)

  be get_pass(f: PassCB iso) =>
    _passwd_provider(consume f)

  be get_user(f: UserCB iso) =>
    f(_user)

  be terminate() =>
    for i in Range(0, _connections.size()) do
      try _connections.pop()?.terminate() end
    end


"""
pg.pony

Do pg stuff.
"""
//use "options"
use "debug"

use "pg/protocol"
use "pg/connection"
use "logger"


interface _StringCB
  fun apply(s: String)
interface PassCB is _StringCB
interface UserCB is _StringCB

type PGValue is (I64 | I32 | None)

type Param is (String, String)

actor Session
  let _env: Env
  let _mgr: ConnectionManager
  let logger: Logger[String val] val

  new create(env: Env,
             host: (String | None) = None,
             service: (String| None) = None,
             user: (String | None) = None,
             password: (String | PasswordProvider tag| None) = None,
             database: (String | None) = None
             ) =>
    _env = env
    logger = StringLogger(Fine, env.out)

    // retreive the connection parameters from env if not provided
    // TODO: we should implement all options of libpq as well :
    // https://www.postgresql.org/docs/current/static/libpq-envars.html

    let user' = "red" //try
//      user as String
//    else try
//      EnvVars(env.vars())("PGUSER")
//    else try
//      EnvVars(env.vars())("USER")
//    else
//      ""
//    end end end

    let host' = "localhost" //try
//      host as String
//    else try
//      EnvVars(env.vars())("PGHOST")
//    else
//      "localhost"
//    end end

    let service' = "5432" //try
//      service as String
//    else try
//      EnvVars(env.vars())("PGPORT")
//    else
//      "5432"
//    end end

    let database' = "red" //try
//      database as String
//    else try
//      EnvVars(env.vars())("PGDATABASE")
//    else
//      user'
//    end end

    // Define the password strategy
    let provider = RawPasswordProvider("red") //match password
//      | None => EnvPasswordProvider(env)
//      | let p: PasswordProvider tag => p
//      | let s: String => RawPasswordProvider(s)
//      else
//        RawPasswordProvider("")
//      end

    _mgr = ConnectionManager(host', service', user', provider, 
      recover val [("user", user'); ("database", database')] end, env.out)

  be log(msg: String) =>
    _env.out.print(msg)

  be connect(f: {(Connection tag)} val) =>
    _mgr.connect(_env.root, f)
//    try _mgr.connect(_env.root, f) end

  be execute(query: String,
             handler: RecordCB val,
             params: (Array[PGValue] val | None) = None) =>
    let f = recover {(c: Connection)(query, params, handler) =>
        c.execute(query, handler, params)
    } end
    _mgr.connect(_env.root, consume f)
//    try _mgr.connect(_env.root, consume f) end

  be terminate()=>
    _mgr.terminate()


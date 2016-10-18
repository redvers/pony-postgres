"""
main.pony


"""
use "pg"
use "pg/codec"
use "pg/introspect"
use "net"
use "debug"
use "promises"


interface ConnectioHandler is Fulfill[Connection, Connection]

class BlogEntry
  let field1: I32
  let field2: I32
  let field3: I32

  new create(f1: I32, f2: I32, f3: I32) =>
    field1 = f1
    field2 = f2
    field3 = f3

  fun string(): String =>
    "BlogEntry " + field1.string() + " " + field2.string() + " " + field3.string()

class User
  let id: I32
  new create(id': I32) =>
    id = id'


class BlogEntryRecordNotify is FetchNotify
  var entries: (Array[BlogEntry val] trn | Array[BlogEntry val] val) = recover trn Array[BlogEntry val] end
  let view: BlogEntriesView tag
  new iso create(v: BlogEntriesView tag) => view = v
  fun ref descirption(desc: RowDescription) => None
  fun size(): USize => 10
  fun ref batch(r: Array[Record val] val, next: FetchNotifyNext val) =>
    Debug.out("Batch")
    if r.size() == size() then
      next(None)
    end
  fun ref record(r: Record val) =>
    Debug.out(".")
    try
       let e = recover val BlogEntry(
          r(0) as I32,
          2, 3
          /*r(1) as I32,*/
          /*r(2) as I32*/
        ) end
      // Debug.out(e.string())
      /*(entries as Array[BlogEntry val] trn).push(e)*/

    end
  fun ref stop() =>
    try
      entries = recover val entries as Array[BlogEntry val] trn end
      view.entries(entries)
    end


class UserRecordNotify is FetchNotify
  let entries: Array[BlogEntry] = Array[BlogEntry]
  let view: BlogEntriesView tag
  new create(v: BlogEntriesView tag) => view = v
  fun ref descirption(desc: RowDescription) => None

  fun ref batch(b: Array[Record val] val, next: FetchNotifyNext val) =>
    Debug(b.size())
    for r in b.values() do
      try
        view.user(recover User(r("id") as I32) end)
      else
        Debug.out("Error")
      end
    end

  fun ref record(r: Record val) =>
    try
      view.user(recover User(r("id") as I32) end)
    end
  fun ref stop() => None


actor BlogEntriesView
  var _conn: (Connection tag | None) = None
  var _user: ( User val | None ) = None
  let _entries: Promise[Array[BlogEntry val] val] = Promise[Array[BlogEntry val] val]

  be fetch_entries() =>
    try
      Debug("fetch_entries")
      (_conn as Connection).fetch(
        /*"SELECT 1 as user_id, 2, 3 UNION ALL SELECT 4 as user_id, 5, 6 UNION ALL SELECT 7 as user_id, 8, 9",*/
        "SELECT generate_series(0,100)",
        recover BlogEntryRecordNotify(this) end)
    end

  be fetch_user() =>
    try
      (_conn as Connection).fetch(
        "SELECT 1 as id",
        recover UserRecordNotify(this) end)
    end

  be user(u: User iso) =>
    _user = recover val consume u end
    Debug.out("###")
    fetch_entries()

  be render(entries': Array[BlogEntry val] val) =>
    Debug.out("render")
    for p in entries'.values() do
      /*Debug.out(p.string())*/
      None
    end
    try (_conn as Connection).release() end

  be entries(e: Array[BlogEntry val] val) =>
    Debug.out("fetch")
    _entries(e)
    
  be apply(conn: Connection tag) =>
    _conn = conn
    fetch_user()
    _entries.next[BlogEntriesView](recover this~render() end)


actor Main
  let session: Session
  let _env: Env

  new create(env: Env) =>
    _env = env
    session = Session(env where user="macflytest",
                   password=EnvPasswordProvider(env),
                   database="macflytest")
    let that = recover tag this end
    """
    session.execute("SELECT generate_series(0,1)",
             recover val
              lambda(r: Rows val)(that) =>
                that.raw_count(r)
                None
              end
             end)

    session.execute("SELECT 42, 24 as foo;;",
             recover val
              lambda(r: Rows val)(that) =>
                  that.raw_handler(r)
              end
             end)


    session.execute("SELECT $1, $2 as foo",
                    recover val
                      lambda(r: Rows val)(that) =>
                        that.execute_handler(r)
                      end
                    end,
                   recover val [as PGValue: I32(70000), I32(-100000)] end)
    """
    let p = session.connect(recover val
      lambda(c: Connection tag) =>
        BlogEntriesView(c)
      end
    end) 

  be raw_count(rows: Rows val) =>
    _env.out.print(rows.size().string())

  be raw_handler(rows: Rows val) =>
    for row in rows.values() do
      try Debug.out(row(0) as I32) end
      try Debug.out(row("foo") as I32) end
    end

  be execute_handler(rows: Rows val) =>
    for row in rows.values() do
      try Debug.out(row(0) as I32) end
      try Debug.out(row("foo") as I32) end
    end

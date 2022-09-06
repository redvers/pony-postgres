use "buffered"
use "debug"

use "pg"
use "pg/codec/types"

primitive TypeOid
  """
    The type oids are found with:

    SELECT
        oid,
        typname
    FROM                  
        pg_catalog.pg_type
    WHERE                    
        typtype IN ('b', 'p')                                                 
        AND (typelem = 0 OR typname = '_oid' OR typname='_text' OR typlen > 0)
        AND oid <= 9999
    ORDER BY
        oid;

  """
  /*
   * 0 Refers to sending a parameter using the more portable text interface
   *
   * Leaving this infrastructure in place, just in case in the future we have
   * a parameter format we are COMPELLED to use binary encoding for
   *
   */
  fun apply(ptype: (None|Bool|U8|I64|I16|I32|String|F32|F64)): I32 => 0

primitive TypeOids
  fun apply(t: Array[PGValue] val): Array[I32] val =>
    recover val
      let result = Array[I32](t.size())
      for item in t.values() do
        result.push(TypeOid(item))
      end
      result
    end

primitive Decode
  fun apply(type_oid: I32, value: Array[U8] val, format: I16): PGValue ? =>
    if format == 0 then
      DecodeText(type_oid, value)?
    else if format == 1 then
      DecodeBinary(type_oid, value)?
    else
      Debug.out("Unknown fromat" + format.string())
      error
    end end

primitive DecodeText
  fun apply(type_oid: I32, value: Array[U8] val): PGValue ? =>
    match type_oid
    | 16 => if (value.apply(0)? == 't') then true else false end
    | 20 => String.from_array(value).i64()? // int8
    | 21 => String.from_array(value).i16()? // int2
    | 23 => String.from_array(value).i32()? // int4
    | 25 => String.from_array(value)        // text
    | 869 => String.from_array(value)       // host
    | 650 => String.from_array(value)       // cidr
    | 1043 => String.from_array(value)      // varchar
    else
      Debug.out("warn: [DecodeText] Don't know how to return native ponytype for : " + type_oid.string())
      String.from_array(value)
    end

/* We probably should not be taking this approach given that the postgres
 * docs explicitly say that binary formats are not considered portable
 * across versions                                                        */
primitive DecodeBinary
  fun apply(type_oid: I32, value: Array[U8] val): PGValue ? =>
    match type_oid
    | 20 => Int8.binary(value)
    | 23 => Int4.binary(value)
    else
      Debug.out("[DecodeBinary] Unknown type OID: " + type_oid.string()); error
    end


primitive EncodeBinary
  fun apply(param: (I32|PGValue), writer: Writer) ? =>
    match param
    | let x: I32 => writer.i32_be(x)
    | let x: PGValue => error
    end

primitive EncodeText
  fun apply(param: PGValue, writer: Writer) =>
    let str: String val = recover val param.string() end
    let len: I32 = str.size().i32()
    writer.i32_be(len)
    writer.write(str)


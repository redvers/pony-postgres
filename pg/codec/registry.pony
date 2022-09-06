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
  // TODO: Find NULL oid, i'm pretty sure it's not 0
  fun apply(ptype: (None|Bool|U8|I64|I16|I32|String|F32|F64)): I32 =>
    match ptype
    | let x: None => 0
    | let x: Bool => 16
    | let x: U8 => 18
    | let x: I64 => 20
    | let x: I16 => 21
    | let x: I32 => 23
    | let x: String => 25
    | let x: F32 => 700
    | let x: F64 => 701
    end
//  fun apply(t: None): I32 => 0
//  fun apply(t: Bool val): I32 => 16
//  fun apply(t: U8 val): I32 => 18
//  fun apply(t: I64 val): I32 => 20
//  fun apply(t: I16 val): I32 => 21
//  fun apply(t: I32 val): I32 => 23
//  fun apply(t: String val): I32 => 25
//  fun apply(t: F32 val): I32 => 700
//  fun apply(t: F64 val): I32 => 701
  /*fun apply(t: Any val): I32 => 0*/

primitive TypeOids
  fun apply(t: Array[PGValue] val): Array[I32] val =>
    recover val
      let result = Array[I32](t.size())
      for item in t.values() do
        result.push(TypeOid(item))
//        try result.push(TypeOid(item)) end
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
//  fun apply(param: I32, writer: Writer) ? =>
//    writer.i32_be(param)
//  fun apply(param: PGValue, writer: Writer) ? => error

primitive Int8
  fun binary(value: Array[U8] val): I64 =>
    var result: I64 = I64(0)
    for i in value.values() do
      result = (result << 8) + i.i64()
    end
    result

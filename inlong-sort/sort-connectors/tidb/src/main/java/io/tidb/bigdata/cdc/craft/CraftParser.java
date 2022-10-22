/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.cdc.craft;

import io.tidb.bigdata.cdc.Key;
import io.tidb.bigdata.cdc.Parser;
import java.util.ArrayList;
import java.util.Arrays;

public class CraftParser implements Parser<CraftParserState> {

  static final int META_SIZE_TABLE_INDEX = 0;
  static final int VALUE_SIZE_TABLE_INDEX = 1;
  static final int COLUMN_GROUP_SIZE_TABLE_START_INDEX = 2;

  static final int KEY_SIZE_INDEX = 0;
  static final int TERM_DICTIONARY_SIZE_INDEX = 1;

  private static final int CURRENT_VERSION = 1;
  private static final CraftParser INSTANCE = new CraftParser();

  private CraftParser() {
  }

  public static CraftParser getInstance() {
    return INSTANCE;
  }

  private static CraftParserState doParse(Codec codec) {
    if (codec.decodeUvarint() != CURRENT_VERSION) {
      throw new RuntimeException("Illegal version, should be " + CURRENT_VERSION);
    }
    final int[][] sizeTables = parseSizeTables(codec);
    final int[] metaSizeTable = sizeTables[META_SIZE_TABLE_INDEX];
    int numOfPairs = sizeTables[VALUE_SIZE_TABLE_INDEX].length;
    int keyBytes = metaSizeTable[KEY_SIZE_INDEX];
    int keyAndValueBytes =
        Arrays.stream(sizeTables[VALUE_SIZE_TABLE_INDEX]).sum() + keyBytes;
    Codec headerAndBodyCodec = codec.truncateHeading(keyAndValueBytes);
    int termDictionarySize = metaSizeTable[TERM_DICTIONARY_SIZE_INDEX];
    CraftTermDictionary termDictionary = termDictionarySize == 0 ? CraftTermDictionary.empty()
        : new CraftTermDictionary(codec.truncateHeading(metaSizeTable[TERM_DICTIONARY_SIZE_INDEX]));
    Key[] keys = parseKeys(headerAndBodyCodec, numOfPairs, keyBytes, termDictionary);
    return new CraftParserState(headerAndBodyCodec, keys, sizeTables, termDictionary);
  }

  private static Key[] parseKeys(Codec codec, int numOfKeys, int keyBytes,
      CraftTermDictionary termDictionary) {
    Codec keysCodec = codec.truncateHeading(keyBytes);
    long[] ts = keysCodec.decodeDeltaUvarintChunk(numOfKeys);
    long[] type = keysCodec.decodeUvarintChunk(numOfKeys);
    long[] partition = keysCodec.decodeDeltaVarintChunk(numOfKeys);
    String[] schema = termDictionary.decodeNullableChunk(keysCodec, numOfKeys);
    String[] table = termDictionary.decodeNullableChunk(keysCodec, numOfKeys);
    Key[] keys = new Key[numOfKeys];
    for (int idx = 0; idx < numOfKeys; ++idx) {
      keys[idx] = new Key(schema[idx], table[idx], partition[idx], (int) type[idx], ts[idx]);
    }
    return keys;
  }

  private static int[][] parseSizeTables(Codec codec) {
    int size = codec.decodeUvarintReversedLength();
    Codec slice = codec.truncateTailing(size);
    ArrayList<int[]> tables = new ArrayList<>();
    while (slice.available() > 0) {
      int elements = slice.decodeUvarintLength();
      tables.add(
          Arrays.stream(slice.decodeDeltaVarintChunk(elements)).mapToInt(l -> (int) l).toArray());
    }
    return tables.toArray(new int[0][]);
  }

  @Override
  public CraftParserState parse(byte[] bits) {
    return doParse(new Codec(bits));
  }
}
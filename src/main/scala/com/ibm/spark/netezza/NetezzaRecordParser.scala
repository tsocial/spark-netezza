/**
 * (C) Copyright IBM Corp. 2015, 2016
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
 *
 */

package com.ibm.spark.netezza

import org.apache.commons.csv.{CSVFormat, CSVParser, QuoteMode}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Class provides methods to parse the data written by the Netezza into
  * the remote client pipe. Format of the data is controlled by the external
  * table definition options.
  */
class NetezzaRecordParser(
  delimiter: Char,
  escapeChar: Char,
  schema: StructType,
  options: Map[String, String] = Map.empty
) {

  val csvFormat = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.NONE).withDelimiter(delimiter).withEscape(escapeChar)
  val row: NetezzaRow = new NetezzaRow(schema, options)
  private val log = LoggerFactory.getLogger(getClass)

  /**
    * Parse the input String into column values.
    *
    * @param input string value of a row
    * @return row object that contains the current values..
    */
  def parse(input: String): NetezzaRow = {
    val parser = CSVParser.parse(input, csvFormat)
    try {
      val records = parser.getRecords()
      records.isEmpty match {
        case true => {
          // null value for single column select.
          row.setValue(0, "")
        }
        case false => {
          // Parsing is one row at a tine , only one record expected.
          val record = records.get(0)
          for (i: Int <- 0 until record.size()) {
            row.setValue(i, record.get(i))
          }
        }
      }
    } catch {
      case e: Exception => {
        log.error("Failed to parse row: " + input)
        e.printStackTrace()
        throw e
      };
    }
    row
  }
}

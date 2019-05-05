
package org.sh.db

import org.sh.utils.common.file.prop.EncryptorDecryptor
import java.io._
import org.sh.db.core.DataStructures._
import org.sh.utils.common.Util._
import org.h2.tools.Csv

object DBManagerMgmt {
  def ensureCopyable(src:DBManager, dest:DBManager) = {    
    if (src.dbConfig.dbname == dest.getConfig.dbname && 
        src.dbConfig.dbhost == dest.getConfig.dbhost && 
        src.dbConfig.dbms == dest.getConfig.dbms && 
        src.getTable.tableName == dest.getTable.tableName)  //        dest.table.tableName == able.tableName)
      throw new DBException("at least one of dbname, dbhost, dbms or table needs to be different") //    if (this.table != dest.table)
      //      throw new DBException("table mismatch. Both schemas and names need to be identical")
    if (src.table == dest.table) throw new DBException("Both tables are same") //      throw new DBException("table mismatch. Both schemas and names need to be identical")
    if (!src.getTable.tableCols.zip(dest.getTable.tableCols).forall{
        case (l, r) => l.name == r.name && l.colType == r.colType
      }
    ) throw new DBException("table mismatch. Both schemas need to be identical")    
    if (dest.countAllRows > 0) 
      throw new DBException("dest table must be empty")
    if (src.countAllRows < 1)
      throw new DBException("source table must be non-empty")
  }
  // inefficient way to copy
  /**
   * this is inefficient. It inserts one row at a time
   */
  import BetterDB._
  @deprecated def copyFromTo(src:DBManager, dest:DBManager) = {
    ensureCopyable(src, dest)
    src.selectStar.asList foreach dest.insert
    dest.countAllRows
  }
  
  /// check following carefully if they cause issue in secureDBManager (i.e, which tables, cols are getting passed.. encrypted or plaintext?)
  def copyFromToViaFile(src:DBManager, dest:DBManager) = {
    val file = randomAlphanumericString(20)
    src.exportAllToCSV(file)
    dest.importFromCSV(file)
    val deleted = org.sh.utils.common.file.Util.deleteFile(file)
    val info = if (deleted) "File deleted." else "Unable to delete file."
    "Done! Copied via file: "+file+". "+info
  }
  def copyFromToViaEncryptedFile(src:DBManager, dest:DBManager) = {
    val file = randomAlphanumericString(20)
    val secret = randomAlphanumericString(100)
    src.exportEncrypted(file, secret)
    dest.importEncrypted(file, secret)
    val deleted = org.sh.utils.common.file.Util.deleteFile(file)
    val info = if (deleted) "File deleted." else "Unable to delete file."
    "Done! Copied via file: "+file+". "+info
  }
  def getKeyFromSecret(secret:String) = {
    val hash = sha256(secret).take(32)
    val aesKey = hash.substring(0, 16);
    val iv = hash.substring(16);
    (aesKey, iv)
  }
  def exportToEncryptedCSV(src:DBManager, file:String, secretKey:String, wheres: Wheres) = {    
    val (aesKey, iv) = getKeyFromSecret(secretKey)
    val ed = new EncryptorDecryptor(aesKey, iv)    
    val osw = new OutputStreamWriter(new java.util.zip.GZIPOutputStream(ed.getEncryptedStream(new FileOutputStream(new File(file)))))
    val csv = new Csv
    src.selectRS(wheres.toArray, src.getTable.tableCols, csv.write(osw, _))
  }
  def importFromEncryptedCSV(dest:DBManager, file:String, secretKey:String) = {    
    val (aesKey, iv) = getKeyFromSecret(secretKey)
    val ed = new EncryptorDecryptor(aesKey, iv)    
    using(new InputStreamReader(new java.util.zip.GZIPInputStream(ed.getDecryptedStream(new FileInputStream(new File(file)))))){isr =>
      val csv = new Csv
      // from http://www.h2database.com/javadoc/org/h2/tools/Csv.html
      // read(Reader reader, String[] colNames) throws IOException
      //      Reads CSV data from a reader and returns a result set. The rows in the result set are created on demand, that means the reader is kept open until all rows are read or the result set is closed.
      //      Parameters:
      //      reader - the reader
      //      colNames - or null if the column names should be read from the CSV file
      //      Returns:
      //      the result set
      dest.insertRS(csv.read(isr, null))
    }
  }  

}









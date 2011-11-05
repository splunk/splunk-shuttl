package edu.jhu.nlp.util;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * 
 * A miscellaneous collection of file utilities
 * @author Delip Rao
 *
 */
public class FileUtil {
  
  /**
   * Read contents of a file into a string.
   * @param file
   * @return file contents.
   */
  public static String readFile(String fileName) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(fileName));
      StringBuffer sb = new StringBuffer();
      String line = null;
      while((line = br.readLine()) != null) {
        sb.append(line);
        sb.append('\n');
      }
      br.close();
      return sb.toString();
    } catch(Exception e) {
      e.printStackTrace();
    }
    return null;
  }
}

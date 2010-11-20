/**
 * Copyright 2010 Kiran Ratnapu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.diffkit.diff.sns;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;


import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.diffkit.common.DKUserException;
import org.diffkit.common.DKValidate;
import org.diffkit.common.annot.NotThreadSafe;
import org.diffkit.diff.engine.DKColumnModel;
import org.diffkit.diff.engine.DKContext;
import org.diffkit.diff.engine.DKSource;
import org.diffkit.diff.engine.DKTableModel;
import org.diffkit.util.DKArrayUtil;
import org.diffkit.util.DKResourceUtil;

/**
 * @author jpanico
 */
@NotThreadSafe
public class DKSpreadSheetFileSource implements DKSource {

   private final File _file;
   //private final String _delimiter;
   /**
    * read from the first line of actual file
    */
   private String[] _headerColumnNames;
   private DKTableModel _model;
   private final String[] _keyColumnNames;
   /**
    * DKColumnModel indices
    */
   private final int[] _readColumnIdxs;
   private DKColumnModel[] _readColumns;
   private final boolean _isSorted;
   private final boolean _validateLazily;
   private final String _sheetName;
   private int _totalRows;
   private transient long _lineCounter = 0;
   private transient boolean _isOpen;
   private transient long _lastIndex = -1;
   private Sheet _sheet;
   //private transient LineNumberReader _lineReader;
   private final Logger _log = LoggerFactory.getLogger(this.getClass());

   /**
    * @param readColumnIdxs_
    *           instructs Source to only read a subset of columns. null value
    *           means all Columns will be read and must be modelled
    */
   public DKSpreadSheetFileSource(String filePath_, String excelSheetName_, DKTableModel model_, int[] readColumnIdxs_) throws IOException {
      this(filePath_, model_, null, readColumnIdxs_, true, true,excelSheetName_);
   }

   /**
    * @param readColumnIdxs_
    *           instructs Source to only read a subset of columns. null value
    *           means all Columns will be read and must be modelled
    */
   public DKSpreadSheetFileSource(String filePath_, DKTableModel model_, String[] keyColumnNames_,
                       int[] readColumnIdxs_, boolean isSorted_,
                       boolean validateLazily_, String sheetName_) throws IOException {

      _log.debug("filePath_->{}", filePath_);
      _log.debug("model_->{}", model_);
      _log.debug("keyColumnNames_->{}", keyColumnNames_);
      _log.debug("readColumnIdxs_->{}", readColumnIdxs_);
      _log.debug("isSorted_->{}", isSorted_);
      _log.debug("validateLazily_->{}", validateLazily_);
      _log.debug("excelSheetName_->{}", sheetName_);

      if ((model_ != null) && (keyColumnNames_ != null))
         throw new RuntimeException(String.format("does not allow both %s and %s params",
            "model_", "keyColumnNames_"));

      _file = this.getFile(filePath_);
      _model = model_;
      _keyColumnNames = keyColumnNames_;
      _readColumnIdxs = readColumnIdxs_;
      if (_readColumnIdxs != null)
         throw new NotImplementedException(String.format(
            "_readColumnIdxs->%s is not currently supported", _readColumnIdxs));

      _isSorted = isSorted_;
      _validateLazily = validateLazily_;
      _sheetName = sheetName_;
      if (!_isSorted)
         throw new NotImplementedException(String.format(
            "isSorted_->%s is not currently supported", _isSorted));
      if (!_validateLazily) {
         if (_file == null)
            throw new RuntimeException(String.format(
               "could not find file for filePath_->%s", filePath_));
         this.open();
      }
   }

   public File getFile() {
      return _file;
   }


   public DKTableModel getModel() {
      if (_model != null)
         return _model;
      try {
         this.open();
      }
      catch (IOException e_) {
         throw new RuntimeException(e_);
      }
      int[] keyColumnIndices = null;
      if (_keyColumnNames == null)
         keyColumnIndices = new int[] { 0 };
      else
         keyColumnIndices = this.getHeaderColumnNameIndices(_keyColumnNames);

      _model = DKTableModel.createGenericStringModel(_headerColumnNames, keyColumnIndices);
      return _model;
   }

   private int[] getHeaderColumnNameIndices(String[] names_) {
      if (names_ == null)
         return null;
      int[] indices = new int[names_.length];
      Arrays.fill(indices, -1);
      for (int i = 0, j = 0; i < names_.length; i++) {
         int foundAt = ArrayUtils.indexOf(_headerColumnNames, names_[i]);
         if (foundAt < 0)
            throw new RuntimeException(String.format(
               "no value in _headerColumnNames for %s", names_[i]));
         indices[j++] = foundAt;
      }
      return DKArrayUtil.compactFill(indices, -1);
   }

   public String[] getKeyColumnNames() {
      return _keyColumnNames;
   }

   public int[] getReadColumnIdxs() {
      return _readColumnIdxs;
   }

   public boolean getIsSorted() {
      return _isSorted;
   }

   public boolean getValidateLazily() {
      return _validateLazily;
   }

   public Kind getKind() {
      return Kind.FILE;
   }

   public URI getURI() throws IOException {
      return _file.toURI();
   }

   public String toString() {
      return String.format("%s@%x[%s]", ClassUtils.getShortClassName(this.getClass()),
         System.identityHashCode(this), _file.getAbsolutePath());
   }

   public Object[] getNextRow() throws IOException {
      this.ensureOpen();
      Cell[] cells = this.readLine();
      if (cells == null)
         return null;
      _lastIndex++;
      return createRow(cells);
   }

   /**
    * skips blank lines
    * 
    * @return null only when EOF is reached
    */
   private Cell[] readLine() throws IOException {
      while (true) {
         if (_lineCounter == _totalRows)
            return null;
         Row row = _sheet.getRow((int)_lineCounter);
         _log.debug("row:"+ row);
         if(row == null)
        	 return null;
         Cell[] cells = getCells(row);        	 
         _lineCounter++;
         if(!checkIfRowIsEmpty(cells)) {
        	 return cells;
         }     
       }
   }

   private Cell[] getCells(Row row) {
	   Iterator<Cell> iterator = row.cellIterator();
	   if (iterator == null)
		   return null;
	   _log.debug("No of Cells:" + row.getLastCellNum());
	   Cell[] cells = new Cell[row.getLastCellNum()];
	   int i=0;
	   while(iterator.hasNext()) {
		   Cell cell = iterator.next();
		   _log.debug("Cell->", cell);
		   cells[i++] = cell;
	   }
	   _log.debug("Cells->{}", cells);
	   return cells;
   }
   
   private boolean checkIfRowIsEmpty(Cell[] cells) {
	   if (cells == null) {
		   return false;
	   }
	   for(Cell cell:cells){
		   String content = cell.getStringCellValue();
		   if(content != null && !content.trim().equals("")) {
			   return false;
		   }
	   }
	   return true;
   }
   private Object[] createRow(Cell[] cells_) throws IOException {
      if (cells_ == null)
         return null;
      DKColumnModel[] readColumns = this.getReadColumns();
      if (cells_.length != readColumns.length)
         throw new RuntimeException(String.format(
            "columnCount->%s in row->%s does not match modelled table->%s",
            cells_.length, Arrays.toString(cells_), _model));
      try {
         Object[] row = new Object[cells_.length];
         for (int i = 0; i < cells_.length; i++) {
        	 String cellContent = getCellValue(cells_[i]);
        	 
            row[i] = readColumns[i].parseObject(cellContent);
         }
         return row;
      }
      catch (ParseException e_) {
         _log.error(null, e_);
         throw new RuntimeException(e_);
      }
   }

   private String getCellValue(Cell cell) {
	   if(cell == null)
		   return null;
	   int type = cell.getCellType();
	   if(type == Cell.CELL_TYPE_NUMERIC) {
		   String value = String.valueOf(cell.getNumericCellValue());
		   return value;
	   }
	   return cell.toString();
	   
	   
   }
   private DKColumnModel[] getReadColumns() {
      if (_readColumns != null)
         return _readColumns;
      DKTableModel model = this.getModel();
      if (model == null)
         return null;
      _readColumns = model.getColumns();
      return _readColumns;
   }

   public long getLastIndex() {
      return _lastIndex;
   }

   // @Override
   public void close(DKContext context_) throws IOException {
      this.ensureOpen();
      _sheet = null;
      _isOpen = false;
   }

   private File getFile(String filePath_) {
      if (filePath_ == null)
         return null;
      File fsFile = new File(filePath_);
      if (fsFile.exists())
         return fsFile;
      try {
         File resourceFile = DKResourceUtil.findResourceAsFile(filePath_);
         if (resourceFile != null)
            return resourceFile;
      }
      catch (URISyntaxException e_) {
         throw new RuntimeException(e_);
      }
      return fsFile;
   }

   private void validateFile() {
      if (!_file.canRead())
         throw new DKUserException(String.format("can't read file [%s]", _file));
      _log.info("File:" + _file.getAbsolutePath());
   }

   // @Override
   public void open(DKContext context_) throws IOException {
      this.open();
   }

   private void open() throws IOException {
      if (_isOpen)
         return;
      _isOpen = true;
      this.validateFile();
      Workbook workbook =  null;
      try {
    	 
        workbook = WorkbookFactory.create(new  FileInputStream(_file));
      } catch (Exception e_) {
    	  _log.error(null, e_);
    	  throw new IOException(e_);
      }
      _log.info("workbook: " + workbook);
      if (workbook == null) {
         _log.error("couldn't get Workbook for file: " + _file);
      }
      _sheet = workbook.getSheet(_sheetName);
      _log.info("_excelSheet: " + _sheet);
      if (_sheet == null) {
         _log.error("couldn't find sheet named: " + _sheetName);
      }
      _totalRows = _sheet.getLastRowNum(); 
      _log.info("Total Rows in excel file " + _file.getAbsolutePath() + " : " + _totalRows);
      //_lineReader = new LineNumberReader(new BufferedReader(new FileReader(_file)));
      this.readHeader();
   }

   private void readHeader() throws IOException {
      Cell[] headerCells = this.readLine();
      _log.info("header->{}", headerCells);
      if(headerCells == null) {
    	  _log.error("No headers present");
      }
      _headerColumnNames = new String[headerCells.length];
      for(int i=0; i<headerCells.length; i++) {
    	 _headerColumnNames[i] = getCellValue(headerCells[i]);    		  
    	 if(_headerColumnNames[i] == null)
    	    _log.error("no header for header index:" + i);   	  
      }
      _log.debug("_headerColumnNames->{}", _headerColumnNames);
   }

   private void ensureOpen() {
      if (!_isOpen)
         throw new RuntimeException("not open!");
   }
}

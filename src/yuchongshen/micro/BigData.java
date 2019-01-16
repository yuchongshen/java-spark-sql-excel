/**
 * 
 */
package yuchongshen.micro;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
/**
 * 功能:java使用单机版sparksql读取excel
 * @author yuchong.shen
 * 2019年1月15日
 * 
 */
public class BigData {
	//定义SPARK_HOME,本人将spark包解压放在与此项目父级目录下的bigdata下例如
	/**
	 * c:\workspace
	 * 		-stand-alone-spark
	 * 		-bigdata
	 * 			-spark
	 */
	private static final String HADOOP_HOME =System.getProperty("user.dir").substring(0, System.getProperty("user.dir").indexOf("java-spark-sql-excel"))+"bigdata"+File.separatorChar+"hadoop";
	private static final String SPARK_HOME =System.getProperty("user.dir").substring(0, System.getProperty("user.dir").indexOf("java-spark-sql-excel"))+"bigdata"+File.separatorChar+"spark"+File.separatorChar+"spark-warehouse";
	//xxx.0的数值简单认为是整型
	private static final Pattern p = Pattern.compile("\\d+.0$");
	public  static final SparkSession spark;
	//初始化SparkSession
	static  {
		System.setProperty("hadoop.home.dir", HADOOP_HOME);
		spark = SparkSession.builder()
			    .appName("test")
			    .master("local[*]") 
			    .config("spark.sql.warehouse.dir",SPARK_HOME)
			    .config("spark.sql.parquet.binaryAsString", "true")
			    .getOrCreate();
	}
	public static void main(String[] args) throws Exception {
		
		//需要查询的excel路径
		String xlsxPath = "test2.xlsx";
		String xlsPath  = "test.xls";
		//定义表名
		String tableName1="test_table1";		
		String tableName2="test_table2";		
		
		readExcel(xlsxPath,tableName2);
		spark.sql("select * from "+tableName2+"Sheet1").show();
		
		readExcel(xlsPath,tableName1);
		spark.sql("select * from "+tableName1+"Sheet1").show();
		spark.sql("select * from "+tableName1+"Sheet2").show();
		spark.sql("select * from "+tableName1+"Sheet3").show();

	}
	public static void readExcel(String filePath,String tableName) throws IOException{
		 DecimalFormat format = new DecimalFormat(); 
		 format.applyPattern("#");
		//创建文件(可以接收上传的文件，springmvc使用CommonsMultipartFile，jersey可以使用org.glassfish.jersey.media.multipart.FormDataParam（参照本人文件上传博客）)
		File file = new File(filePath);
		//创建文件流
		InputStream inputStream = new FileInputStream(file);
		//创建流的缓冲区
	    BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
	    //定义Excel workbook引用
		Workbook  workbook =null;
	    //.xlsx格式的文件使用XSSFWorkbook子类，xls格式的文件使用HSSFWorkbook
		if(file.getName().contains("xlsx")) workbook = new XSSFWorkbook(bufferedInputStream);
		if(file.getName().contains("xls")&&!file.getName().contains("xlsx"))  workbook = new HSSFWorkbook(bufferedInputStream);
		System.out.println(file.getName());
		//获取Sheets迭代器
		Iterator<Sheet> dataTypeSheets= workbook.sheetIterator();
		while(dataTypeSheets.hasNext()){
			//每一个sheet都是一个表，为每个sheet
			ArrayList<String> schemaList = new ArrayList<String>();
			 // dataList数据集
	        ArrayList<org.apache.spark.sql.Row> dataList = new ArrayList<org.apache.spark.sql.Row>();
	        //字段
	        List<StructField> fields = new ArrayList<>();
	        //获取当前sheet
			Sheet   dataTypeSheet = dataTypeSheets.next();
			//获取第一行作为字段
			Iterator<Row> iterator = dataTypeSheet.iterator();
			//没有下一个sheet跳过
			if(!iterator.hasNext()) continue;
			//获取第一行用于建立表结构
			Iterator<Cell> firstRowCellIterator = iterator.next().iterator();
			 while(firstRowCellIterator.hasNext()){
			     //获取第一行每一列作为字段
				 Cell currentCell = firstRowCellIterator.next();
			     //字符串
			     if(currentCell.getCellTypeEnum() == CellType.STRING) schemaList.add(currentCell.getStringCellValue().trim());
			     //数值
			     if(currentCell.getCellTypeEnum() == CellType.NUMERIC)  schemaList.add((currentCell.getNumericCellValue()+"").trim());
			 }
			 //创建StructField(spark中的字段对象，需要提供字段名，字段类型，第三个参数true表示列可以为空)并填充List<StructField>
			 for (String fieldName : schemaList) {
			   StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			   fields.add(field);
			 }
			 //根据List<StructField>创建spark表结构org.apache.spark.sql.types.StructType
			StructType schema = DataTypes.createStructType(fields);
			//字段数len
			int len = schemaList.size();
			//获取当前sheet数据行数
		    int rowEnd = dataTypeSheet.getLastRowNum(); 
		    //遍历当前sheet所有行
		    for (int rowNum = 1; rowNum <= rowEnd; rowNum++) {  
		       //一行数据做成一个List
			   ArrayList<String> rowDataList = new ArrayList<String>();
			   //获取一行数据
		       Row r = dataTypeSheet.getRow(rowNum); 
		       if(r!=null){
		    	   //根据字段数遍历当前行的单元格
		    	   for (int cn = 0; cn < len; cn++) {  
			          Cell c = r.getCell(cn, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);  
			          if (c == null)  rowDataList.add("0");//空值简单补零
			          if (c != null&&c.getCellTypeEnum() == CellType.STRING)  rowDataList.add(c.getStringCellValue().trim());//字符串
			          if (c != null&&c.getCellTypeEnum() == CellType.NUMERIC){
			             double value = c.getNumericCellValue(); 
			             if (p.matcher(value+"").matches())  rowDataList.add(format.format(value));//不保留小数点
			             if (!p.matcher(value+"").matches()) rowDataList.add(value+"");//保留小数点
			          }
			          }  
			       }  
		        //dataList数据集添加一行
				dataList.add(RowFactory.create(rowDataList.toArray()));
		       }
		    //根据数据和表结构创建临时表
			spark.createDataFrame(dataList, schema).createOrReplaceTempView(tableName+dataTypeSheet.getSheetName());
		    }		    
	}
}

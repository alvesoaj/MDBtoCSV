import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.DataType;
import java.io.File;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;

class Tool {
    public static void main(String[] args) {
        try {
            Database db = DatabaseBuilder.open(new File("./01S8.mdb"));

            Set<String> tableNames = db.getTableNames();

            for(String tableName : tableNames) {
                Table table = db.getTable(tableName);
                // System.out.println("### " + table.getName());

                ArrayList<Column> columns = new ArrayList<>();

                int columnCount = table.getColumnCount();
                int count = 0;
                String header = "";
                for(Column column : table.getColumns()) {
                    // System.out.println("###### " + column.getName());
                    
                    columns.add(column);

                    if (count != columnCount - 1) {
                        header += column.getName() + ",";
                    } else {
                        header += column.getName();
                    }
                    count++;
                }
                // System.out.println(header + "\n");

                Writer writer = null;
                try {
                    writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream("output/" + table.getName() + ".csv"), "utf-8"));
                    writer.write(header + "\n");
                                        
                    for(Row row : table) {
                        // System.out.println("ROW: " + row);
                        String line = "";
                        for (int i=0; i < columnCount; i++) {
                            String value = getValue(columns.get(i).getType(), row, columns.get(i).getName());
                            if (i != columnCount - 1) {
                                line += value + ",";
                            } else {
                                line += value;
                            }
                        }
                        writer.write(line + "\n");
                    }
                } catch (IOException ex) {
                    ex.printStackTrace();
                } finally {
                    try { writer.close(); } catch (Exception ex) { /*ignore*/ }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getValue(DataType dataType, Row row, String columnName) {
        if(dataType == DataType.INT) {
            return String.valueOf(row.getShort(columnName));
        } else if(dataType == DataType.LONG) {
            return String.valueOf(row.getInt(columnName));
        } else if(dataType == DataType.FLOAT) {
            return String.valueOf(row.getFloat(columnName));
        } else if(dataType == DataType.DOUBLE) {
            return String.format(Locale.US, "%.2f", row.getDouble(columnName));
        } else if(dataType == DataType.NUMERIC) {
            return String.valueOf(row.getBigDecimal(columnName));
        } else if(dataType == DataType.SHORT_DATE_TIME) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyy-mm-dd'T'hh:mm:ss"); 
            return simpleDateFormat.format(row.getDate(columnName));
        } else if(dataType == DataType.TEXT) {
            return row.getString(columnName);
        } else {
            try {
                return row.getString(columnName);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("DT: " + dataType);
                System.out.println("Row: " + row);
                System.out.println("CN: " + columnName);
            } finally {
                return "";
            }
        }
    }
}
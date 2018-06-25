package com.example.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ImpalaDao {
    private static String DRIVER = "com.cloudera.impala.jdbc41.Driver";
    private static String URL = "jdbc:impala://10.10.30.203:21050/external_partitions";

    public static void main(String[] args) {
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement pst = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL);
            pst = conn.prepareStatement("SELECT tab2.*\n" +
                    "FROM tab2,\n" +
                    "(SELECT tab1.col_1, MAX(tab2.col_2) AS max_col2\n" +
                    " FROM tab2, tab1\n" +
                    " WHERE tab1.id = tab2.id\n" +
                    " GROUP BY col_1) subquery1\n" +
                    "WHERE subquery1.max_col2 = tab2.col_2;");
//            pst = conn.prepareStatement("select * from logs");
            rs = pst.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
                pst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
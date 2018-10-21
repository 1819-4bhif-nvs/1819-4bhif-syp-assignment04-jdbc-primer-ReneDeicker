package at.htl.cardealermanagement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class CarDealerManagementTest {
    public static final String DRIVER_STRING = "org.apache.derby.jdbc.ClientDriver";
    public static final String CONNECTION_STRING = "jdbc:derby://localhost:1527/db;create=true";
    public static final String USER = "app";
    public static final String PASSWORD = "app";
    public static Connection conn;

    @BeforeClass
    public static void initJdbc(){
        try {
            Class.forName(DRIVER_STRING);
            conn = DriverManager.getConnection(CONNECTION_STRING, USER, PASSWORD);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Verbindung zur Datenbank nicht möglich\n" + e.getMessage() +"\n");
            System.exit(1);
        }
        Statement stmt;
        String sql;
        try{
            stmt = conn.createStatement();

            sql = "Create Table CARDEALER(" +
                    " ID INT constraint cardealer_pk PRIMARY KEY" +
                    " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    " CITY varchar(20)," +
                    " STREET varchar(20)," +
                    " ZIPCODE INT," +
                    " HOUSENUMBER varchar(10)," +
                    " NAME varchar(30)" +
                    " )";
            stmt.execute(sql);
            System.out.println("Tabelle CARDEALER erstellt");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try{
            stmt = conn.createStatement();
            sql = "Create Table CARTYPE(" +
                    " ID INT constraint cartype_pk PRIMARY KEY" +
                    " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    " NAME varchar(20)," +
                    " PRODUCER varchar(20)" +
                    " )";
            stmt.execute(sql);
            System.out.println("Tabelle CARTYPE erstellt");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            stmt = conn.createStatement();
            sql = "Create Table CAREXEMPLAR(" +
                    " ID INT constraint carexemplar_pk PRIMARY KEY" +
                    " GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)," +
                    " CARDEALER_ID INT NOT NULL ," +
                    " CARTYPE_ID INT NOT NULL ," +
                    " COLOUR varchar(20)," +
                    " HORSEPOWER INT ," +
                    " FOREIGN KEY (CARDEALER_ID) REFERENCES CARDEALER(ID)," +
                    " FOREIGN KEY (CARTYPE_ID) REFERENCES CARTYPE(ID)" +
                    " )";
            stmt.execute(sql);
            System.out.println("Tabelle CAREXEMPLAR erstellt");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    @Test
    public void t0010DML(){
        int countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO CARDEALER (CITY,STREET, ZIPCODE, HOUSENUMBER, NAME) " +
                    "values ('Stadt 1', 'Straße 1', 1324, '1', 'Händler 1')";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CARDEALER (CITY,STREET, ZIPCODE, HOUSENUMBER, NAME) " +
                    "values ('Stadt 2', 'Straße 2', 2345 ,'2', 'Händler 2')";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CARDEALER (CITY,STREET, ZIPCODE, HOUSENUMBER, NAME) " +
                    "values ('Stadt 3', 'Straße 3', 3456, '3', 'Händler 3')";
            countInserts +=stmt.executeUpdate(sql);
            System.out.println("\nDaten in Tabelle CARDEALER eingefügt");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));

        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT CITY, STREET, ZIPCODE, HOUSENUMBER, NAME from CARDEALER");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("CITY"), is("Stadt 1"));
            assertThat(rs.getString("STREET"), is("Straße 1"));
            assertThat(rs.getInt("ZIPCODE"), is(1324));
            assertThat(rs.getString("HOUSENUMBER"), is("1"));
            assertThat(rs.getString("NAME"), is("Händler 1"));
            rs.next();
            assertThat(rs.getString("CITY"), is("Stadt 2"));
            assertThat(rs.getString("STREET"), is("Straße 2"));
            assertThat(rs.getInt("ZIPCODE"), is(2345));
            assertThat(rs.getString("HOUSENUMBER"), is("2"));
            assertThat(rs.getString("NAME"), is("Händler 2"));
            rs.next();
            assertThat(rs.getString("CITY"), is("Stadt 3"));
            assertThat(rs.getString("STREET"), is("Straße 3"));
            assertThat(rs.getInt("ZIPCODE"), is(3456));
            assertThat(rs.getString("HOUSENUMBER"), is("3"));
            assertThat(rs.getString("NAME"), is("Händler 3"));
        } catch (SQLException e) {
            e.printStackTrace();
        }




        countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO CARTYPE (NAME, PRODUCER) " +
                    "values ('Händler 1', 'VW')";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CARTYPE (NAME, PRODUCER) " +
                    "values ('Händler 2', 'Audi')";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CARTYPE (NAME, PRODUCER) " +
                    "values ('Händler 3', 'Bmw')";
            countInserts +=stmt.executeUpdate(sql);
            System.out.println("Daten in Tabelle CARTYPE eingefügt");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));
        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT NAME, PRODUCER from CARTYPE");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getString("NAME"), is("Händler 1"));
            assertThat(rs.getString("PRODUCER"), is("VW"));
            rs.next();
            assertThat(rs.getString("NAME"), is("Händler 2"));
            assertThat(rs.getString("PRODUCER"), is("Audi"));
            rs.next();
            assertThat(rs.getString("NAME"), is("Händler 3"));
            assertThat(rs.getString("PRODUCER"), is("Bmw"));
        } catch (SQLException e) {
            e.printStackTrace();
        }





        countInserts = 0;
        try {
            Statement stmt = conn.createStatement();
            String sql = "INSERT INTO CAREXEMPLAR (CARDEALER_ID, CARTYPE_ID, COLOUR, HORSEPOWER) " +
                    "values (1, 1, 'Rot', 105)";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CAREXEMPLAR (CARDEALER_ID, CARTYPE_ID, COLOUR, HORSEPOWER) " +
                    "values (2, 2, 'Grün', 220)";
            countInserts +=stmt.executeUpdate(sql);
            sql = "INSERT INTO CAREXEMPLAR (CARDEALER_ID, CARTYPE_ID, COLOUR, HORSEPOWER) " +
                    "values (3, 3, 'Blau', 235)";
            countInserts +=stmt.executeUpdate(sql);
            System.out.println("Daten in Tabelle CARTYPE eingefügt\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        assertThat(countInserts, is(3));
        try {
            PreparedStatement pstmt = conn.prepareStatement("SELECT CARDEALER_ID, CARTYPE_ID, COLOUR, HORSEPOWER from CAREXEMPLAR");
            ResultSet rs = pstmt.executeQuery();

            rs.next();
            assertThat(rs.getInt("CARDEALER_ID"), is(1));
            assertThat(rs.getInt("CARTYPE_ID"), is(1));
            assertThat(rs.getString("COLOUR"), is("Rot"));
            assertThat(rs.getInt("HORSEPOWER"), is(105));
            rs.next();
            assertThat(rs.getInt("CARDEALER_ID"), is(2));
            assertThat(rs.getInt("CARTYPE_ID"), is(2));
            assertThat(rs.getString("COLOUR"), is("Grün"));
            assertThat(rs.getInt("HORSEPOWER"), is(220));
            rs.next();
            assertThat(rs.getInt("CARDEALER_ID"), is(3));
            assertThat(rs.getInt("CARTYPE_ID"), is(3));
            assertThat(rs.getString("COLOUR"), is("Blau"));
            assertThat(rs.getInt("HORSEPOWER"), is(235));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void teardownJdbc(){
        try {
            conn.createStatement().execute("Drop table CAREXEMPLAR");
            System.out.println("Tabelle CAREXEMPLAR gelöscht");
        } catch (SQLException e) {
            System.err.println("Tabelle CAREXEMPLAR konnte nicht gelöscht werden\n"+e.getMessage());
        }

        try{
            conn.createStatement().execute("Drop table CARTYPE");
            System.out.println("Tabelle CARTYPE gelöscht");
        } catch (SQLException e) {
            System.err.println("Tabelle CARTYPE konnte nicht gelöscht werden\n"+e.getMessage());
        }

        try{
            conn.createStatement().execute("Drop table CARDEALER");
            System.out.println("Tabelle CARDEALER gelöscht");
        } catch (SQLException e) {
            System.err.println("Tabelle CARDEALER konnte nicht gelöscht werden\n"+e.getMessage());
        }

        try {
            if (conn != null && !conn.isClosed()){
                conn.close();
                System.out.println("\nGood bye");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}

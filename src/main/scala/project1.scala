import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level



object project1
{
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    println("created spark session")

    /*spark.sql("DROP TABLE IF EXISTS kepler_data")
      spark.sql("CREATE TABLE kepler_data(Sat_num Int, Sat_ID String, Semi_Major_Axis Double, Eccentricity Double, Node Double, Inclination Double, Argument_of_perigee Double, True_Anomaly Double, Time Double) row format delimited fields terminated by ','")
      spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/samli/IdeaProjects/project1/input/satdata.csv' INTO TABLE kepler_data")



      //Node=o  Inclination=i Argument of perigee=w
      spark.sql("DROP VIEW IF EXISTS cs_values")
      spark.sql("CREATE VIEW cs_values AS " +
        "SELECT Sat_num AS Sat_num, COS(Node*PI()/180) AS c_node, " +
        "SIN(Node*PI()/180) AS s_node, " +
        "COS(Inclination*PI()/180) AS c_inc, " +
        "SIN(Inclination*PI()/180) AS s_inc, " +
        "COS(Argument_of_Perigee*PI()/180) AS c_per, " +
        "SIN(Argument_of_perigee*PI()/180) AS s_per, " +
        "COS(True_Anomaly*PI()/180) AS c_nu, " +
        "SIN(True_Anomaly*PI()/180) AS s_nu " +
        "FROM kepler_data ORDER BY Sat_num")


      spark.sql("DROP VIEW IF EXISTS transform")
      spark.sql("CREATE VIEW transform AS " +
      "SELECT Sat_num AS Sat_num, (c_node*c_per-c_inc*s_node*s_per) AS 1_1, " +
      "(c_per*s_node+c_inc*c_node*s_per) AS 1_2, " +
      "(c_per*s_inc) AS 1_3, " +
      "(-c_node*s_per-c_inc*c_per*s_node) AS 2_1, " +
      "(c_inc*c_node*c_per-s_node*s_per) AS 2_2, " +
      "(c_per*s_inc) AS 2_3, " +
      "(s_inc*s_node) AS 3_1, " +
      "(-c_node*s_inc) AS 3_2, " +
      "(c_inc) AS 3_3 " +
      "FROM cs_values ORDER BY Sat_num")

      spark.sql("DROP VIEW IF EXISTS RV")
      spark.sql("CREATE VIEW RV AS " +
      "SELECT kepler_data.Sat_num AS Sat_num, " +
      "kepler_data.Semi_Major_Axis*(1-kepler_data.Eccentricity*kepler_data.Eccentricity)*cs_values.c_nu/(1+kepler_data.Eccentricity*cs_values.c_nu) AS R_x, " +
      "kepler_data.Semi_Major_Axis*(1-kepler_data.Eccentricity*kepler_data.Eccentricity)*cs_values.s_nu/(1+kepler_data.Eccentricity*cs_values.c_nu) AS R_y, " +
      "SQRT(398600/kepler_data.Semi_Major_Axis*(1-kepler_data.Eccentricity*kepler_data.Eccentricity))*-cs_values.s_nu AS V_x, " +
      "SQRT(398600/kepler_data.Semi_Major_Axis*(1-kepler_data.Eccentricity*kepler_data.Eccentricity))*(kepler_data.Eccentricity+cs_values.c_nu) AS V_y " +
      "FROM kepler_data JOIN cs_values ON (kepler_data.Sat_num=cs_values.Sat_num) ORDER BY Sat_num")

      spark.sql("DROP VIEW IF EXISTS Inertial")
      spark.sql("CREATE VIEW Inertial AS " +
        "SELECT RV.Sat_num AS Sat_num, " +
        "RV.R_x*transform.1_1+RV.R_y*transform.2_1 AS RX, " +
        "RV.R_x*transform.1_2+RV.R_y*transform.2_2 AS RY, " +
        "RV.R_x*transform.1_3+RV.R_y*transform.2_3 AS RZ, " +
        "RV.V_x*transform.1_1+RV.V_y*transform.2_1 AS VX, " +
        "RV.V_x*transform.1_2+RV.V_y*transform.2_2 AS VY, " +
        "RV.V_x*transform.1_3+RV.V_y*transform.2_3 AS VZ " +
        "FROM RV JOIN transform ON (RV.Sat_num=transform.Sat_num) ORDER BY Sat_num")

      spark.sql("DROP VIEW IF EXISTS rotation")
      spark.sql("Create VIEW rotation AS " +
        "SELECT Sat_num AS Sat_num, " +
        "99.4033*PI()/180+time*0.00007292 AS ga " +
        "FROM kepler_data")

      spark.sql("DROP VIEW IF EXISTS geo")
      spark.sql("CREATE VIEW geo AS " +
      "SELECT Inertial.Sat_num AS Sat_num, " +
      "Inertial.RX*COS(rotation.ga)-Inertial.RY*SIN(rotation.ga) AS geo_rx, " +
      "Inertial.RX*SIN(rotation.ga)+Inertial.RY*COS(rotation.ga) AS geo_ry, " +
      "Inertial.RZ AS geo_rz " +
      "FROM Inertial JOIN rotation ON (Inertial.Sat_num=rotation.Sat_num) ORDER BY Sat_num")

      spark.sql("DROP VIEW IF EXISTS latlong")
      spark.sql("CREATE VIEW latlong AS " +
      "SELECT kepler_data.Sat_num as Sat_num, " +
        "ATAN2(geo.geo_ry,geo.geo_rx)*180/PI() AS Longitude, " +
        "1/SIN(geo.geo_rz/(kepler_data.Semi_Major_Axis*(1-Eccentricity)))*180/PI() AS Latitude " +
        "FROM kepler_data JOIN geo ON (kepler_data.Sat_num=geo.Sat_num) ORDER By Sat_num")

      spark.sql("DROP VIEW IF EXISTS Useful_Info")
      spark.sql("CREATE VIEW Useful_Info As " +
        "SELECT Inertial.Sat_num AS Sat_num, " +
        "latlong.Latitude AS Latitude, " +
        "latlong.Longitude AS Longitude, " +
        "SQRT(Inertial.RX*Inertial.RX+Inertial.RY*Inertial.RY+Inertial.RZ*Inertial.RZ)-6378.1 AS Altitude, " +
        "SQRT(Inertial.VX*Inertial.VX+Inertial.VY*Inertial.VY+Inertial.VZ*Inertial.VZ) AS Velocity " +
        "FROM Inertial JOIN latlong ON (Inertial.Sat_num=latlong.Sat_num) ORDER BY Sat_num")

      spark.sql("DROP TABLE IF EXISTS Satellite_Info")
      spark.sql("CREATE TABLE Satellite_Info " +
        "SELECT kepler_data.Sat_num as Sat_num, " +
        "kepler_data.True_Anomaly/360*100 as Percent_Complete, " +
        "Useful_Info.Latitude as Latitude, " +
        "Useful_Info.Longitude as Longitude, " +
        "Useful_Info.Altitude as Altitude, " +
        "Useful_Info.Velocity as Velocity " +
        "FROM kepler_data JOIN Useful_Info ON kepler_data.Sat_num=Useful_Info.Sat_num")*/


    //spark.sql("DROP TABLE IF EXISTS USERS")
    //spark.sql("CREATE TABLE IF NOT EXISTS USERS(user_id int, username String, password String, rank String)")
    //spark.sql("INSERT INTO USERS VALUES('1', 'Cody', 'word', 'admin')")
    //spark.sql("INSERT INTO USERS VALUES('2', 'newguy', 'pass', 'user')")


    println("Login or Register")
    var option = scala.io.StdIn.readLine()
    var boolean = "True"
    var booboo = "True"
    if (option.equalsIgnoreCase("login")) {
    println("What is your username?")
    var user = scala.io.StdIn.readLine()

    println("What is your password?")
    var pass = scala.io.StdIn.readLine()
    var chk = spark.sql("SELECT password FROM USERS Where username='" + user + "'")
    var check = chk.first().getString(0)
    var privl = spark.sql("SELECT rank FROM USERS Where username='" + user + "'")
    var privledge = privl.first().getString(0)
      while (boolean == "True")
      if (pass == check) {
        println("correct")

          if (privledge == "admin") {
            println("What would you like to do?" +
              "\n list (see all users info)" +
              "\n amount (amount of debris)" +
              "\n alt (highest altitude)" +
              "\n orb (shortest orbit progression)" +
              "\n long (closest longitude to 0)" +
              "\n speed (greatest velocity)" +
              "\n future (decay rate)" +
              "\n info(return satellite information)")
            var choice1 = scala.io.StdIn.readLine()
            if (choice1 == "list")
              {spark.sql("SELECT * FROM USERS").show()}
            else if (choice1 == "amount") {
              println("How many pieces of debris are there?")
              spark.sql("SELECT count(*) AS total FROM Satellite_Info").show()
            }
            else if (choice1 == "alt") {
              println("Which piece has the greatest altitude?")
              spark.sql("SELECT MAX(Altitude) as max FROM Satellite_info").show()
            }
            else if (choice1 == "orb") {
              println("Which piece is at the earliest point in its orbital cycle")
              spark.sql("SELECT MIN(Percent_Complete) as min FROM Satellite_Info").show()
            }
            else if (choice1 == "long") {
              println("Which piece is the closest to Greenwich England Longitude (0)")
              spark.sql("SELECT MIN(ABS(Longitude)) as min FROM Satellite_Info").show()
            }
            else if (choice1 == "speed") {
              println("Which piece of debris is moving the fastest?")
              spark.sql("SELECT MAX(Velocity) as max FROM Satellite_Info").show()
            }
            else if (choice1 == "future") {
              println("How Long for each piece to decay?")
              var char1 = spark.sql("SELECT count(Sat_num) FROM kepler_data")
              var chary1 = char1.first().getLong(0).toInt
              spark.sql("SELECT Sat_num, ABS(RZ/VZ) as decay_time FROM Inertial ORDER BY decay_time ASC").show(chary1)
            }
            else if(choice1 == "info")
              {var char = spark.sql("SELECT count(Sat_num) FROM kepler_data")
                var chary = char.first().getLong(0).toInt
               spark.sql("SELECT * FROM kepler_data").show(chary)}
            else if (choice1 == "update")
            {
              println("What is your current username?")
              var chn = scala.io.StdIn.readLine()
              println("New username?")
              var nn = scala.io.StdIn.readLine()
              println("New password?")
              var np = scala.io.StdIn.readLine()
              var id = spark.sql("SELECT user_id FROM USERS WHERE username = '" + chn +"'")
              var idy = id.first().getInt(0)

              spark.sql("DROP VIEW IF EXISTS newuse")
              spark.sql("CREATE VIEW newuse AS " +
                "SELECT * FROM USERS WHERE username != '" + chn + "'")
              spark.sql("DROP TABLE IF EXISTS USER")
              spark.sql("CREATE TABLE USER AS " +
                "SELECT newuse.user_id as user_id, " +
                "newuse.username as username, " +
                "newuse.password as password, " +
                "newuse.rank as rank " +
                "FROM newuse")
              spark.sql("INSERT INTO USER VALUES ('" + idy + "', '" + nn + "', '" + np + "', 'admin')")
              spark.sql("DROP TABLE IF EXISTS USERS")
              spark.sql("ALTER TABLE USER RENAME TO USERS")
            }
            else {println("Invalid")}
            println("Would you like to continue?")
            var boo = scala.io.StdIn.readLine()
            if (boo == "no") {
              boolean = "False"
            }
          }

          else {
            println("What would you like to know?" +
              "\n amount (amount of debris)" +
              "\n alt (highest altitude)" +
              "\n orb (shortest orbit progression)" +
              "\n long (closest longitude to 0)" +
              "\n speed (greatest velocity)" +
              "\n future (decay rate)" +
              "\n info(return satellite information)")
            var choice = scala.io.StdIn.readLine()
            if (choice == "amount") {
              println("How many pieces of debris are there?")
              spark.sql("SELECT count(*) AS total FROM Satellite_Info").show()
            }
            else if (choice == "alt") {
              println("Which piece has the greatest altitude?")
              spark.sql("SELECT MAX(Altitude) as max FROM Satellite_info").show()
            }
            else if (choice == "orb") {
              println("Which piece is at the earliest point in its orbital cycle")
              spark.sql("SELECT MIN(Percent_Complete) as min FROM Satellite_Info").show()
            }
            else if (choice == "long") {
              println("Which piece is the closest to Greenwich England Longitude (0)")
              spark.sql("SELECT MIN(ABS(Longitude)) as min FROM Satellite_Info").show()
            }
            else if (choice == "speed") {
              println("Which piece of debris is moving the fastest?")
              spark.sql("SELECT MAX(Velocity) as max FROM Satellite_Info").show()
            }
            else if (choice == "future") {
              println("How Long for each piece to decay?")
              var char2 = spark.sql("SELECT count(Sat_num) FROM kepler_data")
              var chary2 = char2.first().getLong(0).toInt
              spark.sql("SELECT ABS(RZ/VZ) as decay_time FROM Inertial ORDER BY decay_time ASC").show(chary2)
            }
            else if (choice == "info") {
              println("What is the number of the satellite you would like information on?")
              var infor = scala.io.StdIn.readLine()
              spark.sql("SELECT * FROM Satellite_Info WHERE Sat_num='" + infor + "'").show()
            }
            else if (choice == "update")
            {
              println("What is your current username?")
              var chn = scala.io.StdIn.readLine()
              println("New username?")
              var nn = scala.io.StdIn.readLine()
              println("New password?")
              var np = scala.io.StdIn.readLine()
              var id = spark.sql("SELECT user_id FROM USERS WHERE username = '" + chn +"'")
              var idy = id.first().getInt(0)

              spark.sql("DROP VIEW IF EXISTS newuse")
              spark.sql("CREATE VIEW newuse AS " +
                "SELECT * FROM USERS WHERE username != '" + chn + "'")
              spark.sql("DROP TABLE IF EXISTS USER")
              spark.sql("CREATE TABLE USER AS " +
                "SELECT newuse.user_id as user_id, " +
                "newuse.username as username, " +
                "newuse.password as password, " +
                "newuse.rank as rank " +
                "FROM newuse")
              spark.sql("INSERT INTO USER VALUES ('" + idy + "', '" + nn + "', '" + np + "', 'user')")
              spark.sql("DROP TABLE IF EXISTS USERS")
              spark.sql("ALTER TABLE USER RENAME TO USERS")
            }
            else {println("Invalid")}
            println("Would you like to continue?")
            var boo = scala.io.StdIn.readLine()
            if (boo == "no") {
              boolean = "False"
            }
          }
      }
      else {
        println("incorrect entry try again")
        boolean = "False"
      }
    }
    else if(option.equalsIgnoreCase("register"))
      {
        println("What is your username?")
        var usey = scala.io.StdIn.readLine()
        var counts = spark.sql("SELECT count(username) FROM USERS WHERE username='" + usey + "'")
        var county = counts.first().getLong(0)
        if(county>0){println("Username already taken")}
        else {
          println("What is your password?")
          var passw = scala.io.StdIn.readLine()
          var cnt = spark.sql("SELECT count(*) FROM USERS")
          var cnt2 = cnt.first().getLong(0)
          spark.sql("INSERT INTO USERS VALUES ('" + cnt2 + "', " + "'" + usey + "', '" + passw + "', 'user')")
          println("New user registered sign in to access records")
          boolean = "false"


        }
      }
    else {println("Invalid")}

  }
}





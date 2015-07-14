package v2;

public class DBSetting {
	public String database;
	public String databaseType;
	public String username;
	public String password;
	
	public static DBSetting getLocalDefault() {
		DBSetting s = new DBSetting();
		s.database = "127.0.0.1/seedb_data";
		s.databaseType = "postgresql";
		s.username = "postgres";
		s.password = "postgrespwd";
		return s;
	}
	
	public static DBSetting getDefault() {
		return getLocalDefault();
	}
	
	public static DBSetting getPostgresDefault() {
		DBSetting s = new DBSetting();
		s.database = "vise4.csail.mit.edu:5600/seedb_data";
		s.databaseType = "postgresql";
		s.username = "postgres";
		s.password = "postgrespwd123";
		return s;
	}
	
	public static DBSetting getVerticaDefault() {
		DBSetting s = new DBSetting();
		s.database = "vise4.csail.mit.edu:5433/seedb_data";
		s.databaseType = "vertica";
		s.username = "dbadmin";
		s.password = "dbadmin";
		return s;
	}
}
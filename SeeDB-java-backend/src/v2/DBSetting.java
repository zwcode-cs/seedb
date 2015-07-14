package v2;

public class DBSetting {
	public String database;
	public String databaseType;
	public String username;
	public String password;
	
	public static DBSetting getLocalDefault() {
		DBSetting s = new DBSetting();
		s.database = "";
		s.databaseType ="";
		s.username = "";
		s.password = "";
		return s;
	}
	
	public static DBSetting getDefault() {
		return getLocalDefault();
	}
	
	public static DBSetting getPostgresDefault() {
		DBSetting s = new DBSetting();
		s.database = "";
		s.databaseType = "";
		s.username = "";
		s.password = "";
		return s;
	}
	
	public static DBSetting getVerticaDefault() {
		DBSetting s = new DBSetting();
		s.database = "";
		s.databaseType = "";
		s.username = "";
		s.password = "";
		return s;
	}
}

package common;

public class DBSettings {
	public String database;
	public String databaseType;
	public String username;
	public String password;
	
	public static DBSettings getDefault() {
		DBSettings s = new DBSettings();
		s.database = "dbname";
		s.databaseType = "postgresql";
		s.username = "";
		s.password = "";
		return s;
	}
	
	public static DBSettings getISTCDefault() {
		DBSettings s = new DBSettings();
		s.database = "dbname";
		s.databaseType = "postgresql";
		s.username = "";
		s.password = "";
		return s;
	}
}

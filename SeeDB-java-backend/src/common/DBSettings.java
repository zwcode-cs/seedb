package common;

public class DBSettings {
	public String database;
	public String databaseType;
	public String username;
	public String password;
	
	public static DBSettings getDefault() {
		DBSettings s = new DBSettings();
		s.database = "127.0.0.1/seedb_data";
		s.databaseType = "postgresql";
		s.username = "postgres";
		s.password = "postgrespwd";
		return s;
	}
}

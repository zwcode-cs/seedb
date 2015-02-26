package settings;

public class DBSettings {
	public String database;
	public String databaseType;
	public String username;
	public String password;
	
	public static DBSettings getLocalDefault() {
		DBSettings s = new DBSettings();
		s.database = "127.0.0.1/seedb_data";
		s.databaseType = "postgresql";
		s.username = "postgres";
		s.password = "postgrespwd";
		return s;
	}
	
	public static DBSettings getDefault() {
		return getLocalDefault();
	}
	
	public static DBSettings getPostgresDefault() {
		DBSettings s = new DBSettings();
		s.database = "vise4.csail.mit.edu:5600/seedb_data";
		s.databaseType = "postgresql";
		s.username = "postgres";
		s.password = "postgrespwd123";
		return s;
	}
	
	public static DBSettings getVerticaDefault() {
		DBSettings s = new DBSettings();
		s.database = "vise4.csail.mit.edu:5433/seedb_data";
		s.databaseType = "vertica";
		s.username = "dbadmin";
		s.password = "dbadmin";
		return s;
	}
}

package microbenchmarks;

import java.sql.SQLException;

public class MultipleGroupByTest {
	public static void main(String[] args) {
		String[] tables = new String[] { "s_1"}; // "xs_2", "xs_3", "s_2", "s_3" 
		String[][] attrs = new String[][] { {"dim4_5", "dim11_10", "dim27_50"},
											{"dim5_50", "dim11_10", "dim27_50"}, 
											{"dim5_50", "dim27_50", "dim25_100"}, 
											{"dim5_50", "dim25_100", "dim20_500"},
											{"dim25_100", "dim20_500", "dim24_500"},
											{"dim20_500", "dim24_500", "dim23_1000"}};
		long[] ngroups = new long[] {2500, 25000, 250000, 2500000, 25000000, 250000000};
		for (int mem = 1000; mem <= 1000000; mem=mem*10) {
			for (int i=0; i < attrs.length; i++){
				MultipleGroupBy mgb = new MultipleGroupBy(tables[0], mem, attrs[i], ngroups[i],
						"postgresql", "host/db", "username", "password");
				try {
					mgb.runMultipleGroupByTest();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}	
	}
}

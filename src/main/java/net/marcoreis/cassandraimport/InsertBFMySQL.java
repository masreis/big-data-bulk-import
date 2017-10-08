package net.marcoreis.cassandraimport;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InsertBFMySQL {

	public static void main(String[] args) {
		String url =
				"jdbc:mysql://192.168.25.65:3306/UserDB?rewriteBatchedStatements=true";
		String user = "root";
		String pwd = "root";
		Connection con = null;
		Statement stmt = null;
		try {
			con = DriverManager.getConnection(url, user, pwd);
			stmt = con.createStatement();
			long start = System.currentTimeMillis();
			for (int i = 0; i < 10000; i++) {
				String sql =
						"insert into teste.PagamentoIn values ()";
				stmt.addBatch(sql);
				// execute and commit batch of 1000 queries
				if (i % 1000 == 0)
					stmt.executeBatch();
			}
			// commit remaining queries in the batch
			stmt.executeBatch();

			System.out.println("Time Taken="
					+ (System.currentTimeMillis() - start));

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}

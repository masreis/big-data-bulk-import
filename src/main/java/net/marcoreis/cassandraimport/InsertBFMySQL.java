package net.marcoreis.cassandraimport;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

public class InsertBFMySQL {
	private static Logger logger =
			Logger.getLogger(InsertBFMySQL.class);

	public static void main(String[] args) {
		String filename = args[0];
		new InsertBFMySQL().inserirBatchPrepared(filename);
	}

	public void inserirBatch(String filename) {
		String url = "jdbc:mariadb://192.168.25.65:3306/bf"
				+ "?rewriteBatchedStatements=true";
		String user = "root";
		String pwd = "root";
		Connection con = null;
		Statement stmt = null;
		try {
			Class.forName("org.mariadb.jdbc.Driver");
			con = DriverManager.getConnection(url, user, pwd);
			con.setAutoCommit(false);
			stmt = con.createStatement();
			logger.info("Início");
			try (BufferedReader reader =
					new BufferedReader(new InputStreamReader(
							new FileInputStream(filename),
							Charset.forName("ISO-8859-1")))) {

				reader.readLine();
				String line;
				long ini = System.currentTimeMillis();
				int i = 1;
				while ((line = reader.readLine()) != null) {
					List<String> colunas =
							Arrays.asList(line.split("\t"));
					StringBuilder sql = new StringBuilder(
							"insert into bf.Pagamento values (");
					sql.append("\"").append(colunas.get(0))
							.append("\",");
					sql.append("\"").append(colunas.get(1))
							.append("\",");
					sql.append("\"").append(colunas.get(2))
							.append("\",");
					sql.append("\"").append(colunas.get(3))
							.append("\",");
					sql.append("\"").append(colunas.get(4))
							.append("\",");
					sql.append("\"").append(colunas.get(5))
							.append("\",");
					sql.append("\"").append(colunas.get(6))
							.append("\",");
					sql.append("\"").append(colunas.get(7))
							.append("\",");
					sql.append("\"").append(colunas.get(8))
							.append("\",");
					sql.append("\"").append(colunas.get(9))
							.append("\",");
					sql.append("\"")
							.append(colunas.get(10)
									.replaceAll(",", ""))
							.append("\",");
					sql.append("\"").append(colunas.get(11))
							.append("\")");
					stmt.addBatch(sql.toString());
					if (i % 10000 == 0) {
						stmt.executeBatch();
						con.commit();
						logger.info("Part: " + i);
					}
					i++;
				}
				stmt.executeBatch();
				con.commit();
				long end = System.currentTimeMillis() - ini;
				System.out.println("Tempo de carga: "
						+ (end / (60 * 1000)) + " min");

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void inserirBatchPrepared(String filename) {
		String url = "jdbc:mariadb://192.168.25.65:3306/bf"
				+ "?rewriteBatchedStatements=true";
		String user = "root";
		String pwd = "root";
		Connection con = null;
		PreparedStatement stmt = null;
		try {
			Class.forName("org.mariadb.jdbc.Driver");
			con = DriverManager.getConnection(url, user, pwd);
			con.setAutoCommit(false);
			StringBuilder sql = new StringBuilder(
					"insert into bf.Pagamento values (?,?,?,?,?,?,?,?,?,?,?,?)");
			stmt = con.prepareStatement(sql.toString());
			logger.info("Início");
			try (BufferedReader reader =
					new BufferedReader(new InputStreamReader(
							new FileInputStream(filename),
							Charset.forName("ISO-8859-1")))) {

				reader.readLine();
				String line;
				long ini = System.currentTimeMillis();
				int i = 1;
				while ((line = reader.readLine()) != null) {
					List<String> colunas =
							Arrays.asList(line.split("\t"));
					stmt.setObject(1, colunas.get(0));
					stmt.setObject(2, colunas.get(1));
					stmt.setObject(3, colunas.get(2));
					stmt.setObject(4, colunas.get(3));
					stmt.setObject(5, colunas.get(4));
					stmt.setObject(6, colunas.get(5));
					stmt.setObject(7, colunas.get(6));
					stmt.setObject(8, colunas.get(7));
					stmt.setObject(9, colunas.get(8));
					stmt.setObject(10, colunas.get(9));
					stmt.setObject(11,
							colunas.get(10).replaceAll(",", ""));
					stmt.setObject(12, colunas.get(11));
					stmt.addBatch();
					if (i % 10000 == 0) {
						stmt.executeBatch();
						con.commit();
						logger.info("Part: " + i);
					}
					i++;
				}
				stmt.executeBatch();
				con.commit();
				long end = System.currentTimeMillis() - ini;
				System.out.println("Tempo de carga: "
						+ (end / (60 * 1000)) + " min");

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}

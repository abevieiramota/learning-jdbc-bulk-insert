package br.com.abevieiramota.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class PessoaDAO {

	private Connection conn;

	public PessoaDAO(Connection conn) {

		this.conn = conn;
	}

	public void deleteAll() {
		// TODO: do
	}

	public Integer size() {

		Integer size = null;

		try (Statement stmt = this.conn.createStatement()) {

			ResultSet rs = stmt.executeQuery("select count(*) as total from pessoa");

			rs.next();
			size = rs.getInt("total");
		} catch (SQLException e) {
			// TODO: tratar
			e.printStackTrace();
		}

		return size;
	}

	public void add(Map<String, String> map) {

		try (PreparedStatement pstmt = this.conn.prepareStatement("insert into pessoa values(?, ?)")) {

			pstmt.setString(1, map.get("nome"));
			pstmt.setString(2, map.get("idade"));
			pstmt.execute();
		} catch (SQLException e) {
			// TODO: tratar
			e.printStackTrace();
		}

	}

	public void addBulk(List<Map<String, String>> bulk) {

		try (PreparedStatement pstmt = this.conn.prepareStatement("insert into pessoa values(?, ?)")) {

			for (Map<String, String> map : bulk) {

				pstmt.setString(1, map.get("nome"));
				pstmt.setString(2, map.get("idade"));
				pstmt.addBatch();
			}
			
			pstmt.executeBatch();
		} catch (SQLException e) {
			// TODO: tratar
			e.printStackTrace();
		}

	}

}

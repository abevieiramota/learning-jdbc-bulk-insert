package br.com.abevieiramota.jdbc;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class JdbcTest {

	private static int N_ROWS = 1000000;
	private Connection conn;
	private static String arquivaoFileName = "arquivao.csv";
	private static File arquivao = new File(arquivaoFileName);

	private int bulkSize;

	@Parameters(name = "{index}: bulkSize = {0}")
	public static Collection<Object[]> data() {
		
		return Arrays.asList(new Object[][] {{1}, {10}, {100}, {500}, {1000}, {5000}, {10000}});
	}
	
	public JdbcTest(int bulkSize) {
		
		this.bulkSize = bulkSize;
	}

	@BeforeClass
	public static void beforeClass() {

		criarCSV(arquivao, N_ROWS);
	}

	@AfterClass
	public static void afterClass() {

		deletarArquivo(arquivao);
	}

	@Before // TODO: rs
	public void beforeTest() throws ClassNotFoundException, SQLException {

		this.conn = ConnectionFactory.instance();

		try (Statement stmt = this.conn.createStatement()) {

			stmt.executeQuery("create table pessoa (nome varchar(40), idade int)");
		}
	}

	@After // TODO: rs
	public void afterTest() throws SQLException {

		this.conn.close();
	}

	@Test
	public void testInsert() {

		PessoaDAO pessoaDAO = new PessoaDAO(this.conn);
		pessoaDAO.deleteAll();

		PessoaLoader pessoaLoader = new PessoaLoader();
		pessoaLoader.loadBulk(arquivao, pessoaDAO, this.bulkSize);

		int nPessoas = pessoaDAO.size();

		assertEquals(N_ROWS, nPessoas);
	}

	// --------------- auxiliar ----------------

	private static void deletarArquivo(File file) {

		file.delete();
	}

	private static void criarCSV(File arquivo, int nRows) {

		Object[] header = { "nome", "idade" };
		String nomeTemplate = "Abelardo %d";

		FileWriter fWriter = null;
		CSVPrinter csvFPrinter = null;
		CSVFormat csvFFormat = CSVFormat.DEFAULT.withRecordSeparator('\n');

		try {
			fWriter = new FileWriter(arquivo);
			csvFPrinter = new CSVPrinter(fWriter, csvFFormat);

			csvFPrinter.printRecord(header);

			for (int i = 0; i < nRows; i++) {

				String nome = String.format(nomeTemplate, i);
				int idade = i;
				Object[] record = new Object[] { nome, idade };

				csvFPrinter.printRecord(record);
			}

		} catch (Exception ex) {
			// TODO: tratar
		} finally {
			try {
				fWriter.flush();// TODO: olhar se o close já n executa um flush
				fWriter.close();
				csvFPrinter.close();
			} catch (IOException ioex) {
				// TODO: tratar
			}
		}
	}

}

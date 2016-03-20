package br.com.abevieiramota.jdbc;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class PessoaLoader {

	public void loadUmAUm(File file, PessoaDAO pessoaDAO) {

		try (Reader fReader = new FileReader(file)) {

			CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(fReader);
			
			for (CSVRecord record : parser) {

				pessoaDAO.add(record.toMap());
			}

		} catch (IOException e) {
			// TODO: tratar
		}
	}

	public void loadBulk(File file, PessoaDAO pessoaDAO, int bulkSize) {
		
		List<Map<String, String>> bulk = new ArrayList<Map<String, String>>();
		
		try (Reader fReader = new FileReader(file)) {

			CSVParser parser = CSVFormat.DEFAULT.withHeader().parse(fReader);
			
			for (CSVRecord record : parser) {
				
				bulk.add(record.toMap());
				
				if(bulk.size() == bulkSize) {
					
					pessoaDAO.addBulk(bulk);
					bulk.clear();
				}
			}

		} catch (IOException e) {
			// TODO: tratar
		}
		
	}

}

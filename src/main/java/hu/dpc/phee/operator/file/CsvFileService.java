package hu.dpc.phee.operator.file;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import hu.dpc.phee.operator.entity.batch.Transaction;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CsvFileService {

    @Autowired
    private CsvMapper csvMapper;

    public List<Transaction> getTransactionList(String filename) {
        List<Transaction> transactionList;
        try {
            CsvSchema schema = CsvSchema.emptySchema().withHeader();
            Reader reader = Files.newBufferedReader(Paths.get(filename), Charset.defaultCharset());
            MappingIterator<Transaction> readValues = csvMapper.readerWithSchemaFor(Transaction.class).with(schema).readValues(reader);
            transactionList = new ArrayList<>();
            while (readValues.hasNext()) {
                Transaction current = readValues.next();
                transactionList.add(current);
            }
        } catch (IOException e) {
            log.debug(e.getMessage());
            log.error("Error building TransactionList for file: {}", filename);
            return null;
        }
        return transactionList;
    }

}

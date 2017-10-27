package com.example.service;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.*;

@Service
public class Processor {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    @Autowired
    private Sender sender;

    public void readFile() {
        FileInputStream fileInputStream = null;

        try {
            final URL url = ClassLoader.getSystemResource("initialLoad.xlsx");
            LOG.info("Processing file: {}", url.getFile());

            if (url != null) {
                fileInputStream = new FileInputStream(url.getFile());
                final Workbook workbook = new XSSFWorkbook(fileInputStream);
                final Sheet dataTypeSheet = workbook.getSheetAt(0);
                final Iterator<Row> sheet = dataTypeSheet.iterator();
                final Map<Integer, List<Integer>> map = new HashMap<>();

                while (sheet.hasNext()) {
                    final Row currentRow = sheet.next();
                    final Iterator<Cell> cellIterator = currentRow.iterator();
                    final List<Integer> values = new LinkedList<>();

                    while (cellIterator.hasNext()) {
                        final Cell currentCell = cellIterator.next();

                        if (currentCell.getCellTypeEnum() == CellType.NUMERIC) {
                            values.add((int) currentCell.getNumericCellValue());
                        }
                    }

                    if (!values.isEmpty()) {
                        map.put(currentRow.getRowNum(), values);
                    }
                }

                sender.send(map);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }
}

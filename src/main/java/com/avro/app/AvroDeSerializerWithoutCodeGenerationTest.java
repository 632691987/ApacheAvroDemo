package com.avro.app;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

public class AvroDeSerializerWithoutCodeGenerationTest {

    public static void main(String[] args) throws IOException {
        String avscFilePath = AvroDeSerializerWithoutCodeGenerationTest.class.getClassLoader().getResource("user.avsc").getPath();
        Schema schema = new Schema.Parser().parse(new File(avscFilePath));
        File file = new File("users.avro");
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord user = null;
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }
    }
}
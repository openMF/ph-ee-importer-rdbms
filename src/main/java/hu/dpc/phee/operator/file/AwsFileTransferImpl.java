package hu.dpc.phee.operator.file;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

@Service
public class AwsFileTransferImpl implements FileTransferService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private AmazonS3 s3Client;

    @Override
    public String downloadFile(String fileName, String bucketName) {
        S3Object s3Object = s3Client.getObject(bucketName, fileName);
        try (S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
            byte[] content = IOUtils.toByteArray(inputStream);
            File file = new File(fileName);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(content);
            }
            logger.debug("File {} downloaded at path {}", fileName, file.getAbsolutePath());
            return file.getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException("Failed to download file from S3", e);
        }
    }
}

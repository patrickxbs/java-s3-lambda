package com.patrick;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class ImageGrayscaleHandler implements RequestHandler<S3Event, String> {

    private final S3Client s3Client = S3Client.builder().build();

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        s3Event.getRecords().forEach(record -> {

            String bucketName = record.getS3().getBucket().getName();
            String key = record.getS3().getObject().getKey();

            context.getLogger().log("Processando arquivo: " + key);

            // verifica se a imagem ta na "pasta" original
            if (!key.startsWith("original/")) {
                context.getLogger().log("Arquivo fora da pasta original: " + key);
                return;
            }

            try {
                BufferedImage originalImage = downloadImageFromS3(bucketName, key);

                if (originalImage != null) {
                    BufferedImage blackAndWhiteImg = convertToGrayscale(originalImage);
                    String newKey = uploadImageToS3(bucketName, key, blackAndWhiteImg);
                    context.getLogger().log("Sucesso! Imagem salva em: " + newKey);
                }

            } catch (IOException e) {
                context.getLogger().log("Erro de IO: " + e.getMessage());
                throw new RuntimeException(e);
            }
        });

        return "Ok";
    }

    private BufferedImage downloadImageFromS3(String bucketName, String key) throws IOException {
        InputStream s3Object = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
        return ImageIO.read(s3Object);
    }

    private BufferedImage convertToGrayscale(BufferedImage originalImage) {
        BufferedImage blackAndWhiteImg = new BufferedImage(
                originalImage.getWidth(),
                originalImage.getHeight(),
                BufferedImage.TYPE_BYTE_GRAY);
        blackAndWhiteImg.getGraphics().drawImage(originalImage, 0, 0, null);

        return blackAndWhiteImg;
    }

    private String uploadImageToS3(String bucketName, String key, BufferedImage image) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", outputStream);

        String newKey = key.replace("original/", "processed/");

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(newKey)
                .build();

        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(outputStream.toByteArray()));
        return newKey;
    }
}

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.internal.Mimetypes;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipInputStream;

/**
 * S3 업로드 후 AwsLambda를 이옹하여 unzip 및 unzip 정보를 DataBase에 적재하기
 */
public class AwsLambdaFunction implements RequestHandler<S3Event, String>  {
    private final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final String FAIL = "FAIL";

    //  zip 파일 Path
    String ZIP_PATH;

    //  Image Count
    int IMG_CNT = 0;

    @Override
    public String handleRequest(S3Event input, Context context) {
        context.getLogger().log(String.format("EVENT : %s", gson.toJson(input)));
        S3EventNotification.S3EventNotificationRecord record = input.getRecords().get(0);

        String s3Bucket = record.getS3().getBucket().getName();
        String s3Key = record.getS3().getObject().getKey();

        //  1. ZIP File Validate
        if(!isZipFile(s3Key, context)) return FAIL;

        //  2. Zip -> Unzip
        if(!unzipFile(s3Bucket, s3Key, context)) return FAIL;

        //  3. Zip File Delete
        if(!deleteZipFile(s3Bucket, s3Key, context)) return FAIL;

        //  4. Insert DataBase
        final Properties CONNECT_PROPERTIES = new Properties();
        CONNECT_PROPERTIES.put("user", "DATABASE 계정");
        CONNECT_PROPERTIES.put("password", "DATABASE 비밀번호");
        if(!insertDataBase(context, CONNECT_PROPERTIES)) return FAIL;

        return "SUCCESS";
    }

    /**
     * Zip File -> Unzip File(Image List)
     * @param s3Bucket s3Bucket값
     * @param s3Key s3Key값
     * @param context lambda Context Object
     * @return
     */
    private boolean unzipFile(String s3Bucket, String s3Key, Context context) {
        context.getLogger().log(String.format("[%s] Zip File To Unzip", s3Key));

        S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                s3Bucket, s3Key));



        try (ZipInputStream zipInputStream = new ZipInputStream(s3Object.getObjectContent());) {

            String saveFileName;
            ObjectMetadata objectMetadata;
            byte[] bytes;
            ByteArrayInputStream byteArrayInputStream;

            while (zipInputStream.getNextEntry() != null) {
                //  Save File명 지정
                saveFileName = String.format("%s/proof_img_%s.png", ZIP_PATH, IMG_CNT);
                //  파일마다의 ObjectMetaData 생성
                objectMetadata = new ObjectMetadata();
                //  InputStream To Byte Array
                bytes = IOUtils.toByteArray(zipInputStream);

                //  ObjectMetaData Set Info
                objectMetadata.setContentLength(bytes.length);
                objectMetadata.setContentType(Mimetypes.getInstance().getMimetype(saveFileName));

                //  Generate ByteInputStream
                byteArrayInputStream = new ByteArrayInputStream(bytes);

                //  Request S3Client Put Object
                s3Client.putObject(s3Bucket, saveFileName, byteArrayInputStream, objectMetadata);

                IMG_CNT++;
            }
            zipInputStream.closeEntry();
        } catch (IOException e) {
            context.getLogger().log(e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Zip 파일인지 Check
     * @param s3Key s3 key String
     * @param context
     * @return
     */
    private boolean isZipFile(String s3Key, Context context) {
        context.getLogger().log(String.format("[%s] isZipFile Check", s3Key));

        //  File Type Check
        Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(s3Key);
        if (!matcher.matches()) {
            context.getLogger().log(String.format("Invalid File Type %s", s3Key));
            return false;
        }
        String extension = matcher.group(1).toLowerCase();
        if (!"zip".equals(extension)) {
            context.getLogger().log(String.format("[%s] is not zip file, type = %s", s3Key, extension));
            return false;
        }

        ZIP_PATH = s3Key.substring(0, s3Key.lastIndexOf("/"));
        return true;
    }

    /**
     * Delete Zip File
     * @param s3Bucket s3Bucket값
     * @param s3Key s3Key값
     * @param context lambda Context Object
     */
    private boolean deleteZipFile(String s3Bucket, String s3Key, Context context) {
        try {
            s3Client.deleteObject(s3Bucket, s3Key);
            context.getLogger().log(String.format("Deleted zip file %s/%s", s3Bucket, s3Key));
            return true;
        }
        catch (Exception e) {
            context.getLogger().log(e.getMessage());
        }
        return false;
    }

    /**
     * DataBase Insert
     * @param context lambda Context Object
     * @param properties jdbc 접속 정보
     * @throws SQLException
     */
    private boolean insertDataBase(Context context, Properties properties) {
        context.getLogger().log(String.format("[%s] insertDataBase", properties));

        //  Insert SQL문
        final String sql = "INSERT INTO proof_video(proof_id, type, length, path, image_count) VALUES(?, ?, ?, ?, ?)";

        try(Connection con = DriverManager.getConnection("JDBC URL", properties);
            PreparedStatement preparedStatement = con.prepareStatement(sql)) {

            final String[] fileInfo = ZIP_PATH.split("/")[3].split("_");

            final String PROOF_ID = fileInfo[0];
            final String TYPE     = fileInfo[1];
            final String length   = fileInfo[2];

            preparedStatement.setString(1, PROOF_ID);
            preparedStatement.setString(2, TYPE);
            preparedStatement.setInt(3, Integer.parseInt(length));
            preparedStatement.setString(4, ZIP_PATH);
            preparedStatement.setInt(5, IMG_CNT);

            preparedStatement.executeUpdate();
            return true;
        }
        catch(SQLException exception) {
            context.getLogger().log(exception.getMessage());
        }
        return false;
    }
}

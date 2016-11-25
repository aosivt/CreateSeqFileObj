package aos.Reseach;

import com.sun.media.imageio.stream.FileChannelImageInputStream;
import com.sun.media.jai.codec.ByteArraySeekableStream;
import com.sun.media.jai.codec.ImageCodec;
import com.sun.media.jai.codec.ImageDecoder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;
import javax.media.jai.JAI;
import java.awt.*;
import java.awt.image.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import scala.annotation.meta.param;

/**
 * Created by alex on 07.11.16.
 */
public class CreateSeqFromLargeTiff {
    public static Configuration conf;
    public static FileSystem hdfs;
    public static String str_path;
    public void funcCreateSeqFromTiff() throws IOException {
        Date StartPro = new Date(System.currentTimeMillis());
        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-mm-dd\'T\'HH:mm:ss");
        byte band_number = 0;

        conf = new Configuration();
        hdfs = FileSystem.get(conf);
        str_path = "input";
        Path inFile = new Path("input");
        Path output = null;
        //BufferedWriter br = null;



        FileStatus[] status = hdfs.listStatus(inFile);

        for (int i = 0; i < status.length; i++)
            if (status[i].getPath().getName().indexOf("_001") > 0 ||
                    status[i].getPath().getName().indexOf("B4") > 0) {

                getdatatiff(status[i]);
            }

    }
    public static void getdatatiff(FileStatus _fileStatus) throws IOException {

        FSDataInputStream _Stream = null;
        Path path = new Path(str_path + File.separator +
                _fileStatus.getPath().getName().toString());
        try {
            _Stream = hdfs.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }

        InputStream is = _Stream.getWrappedStream();
        _Stream = null;
        ImageInputStream _stream = new MemoryCacheImageInputStream(is);

        Iterator<ImageReader> readers = ImageIO.getImageReaders(_stream);
            int h = 0;
            int w = 0;
            Rectangle sourceRegion = null;
        if (readers.hasNext()) {
            Writer writer = null;
            Path outPath = new Path("outputfile/test.hsf");
            Option optPath = Writer.file(outPath);
            Option optKey = Writer.keyClass(IntWritable.class);
            Option optVal = Writer.valueClass(BytesWritable.class);
            Option optCom = Writer.compression(SequenceFile.CompressionType.NONE);
            writer = SequenceFile.createWriter(conf, new Option[]{optPath, optKey, optVal, optCom});

            ImageReader reader = readers.next();

            reader.setInput(_stream);
            readers = null;
            _stream = null;
            BufferedImage image = null;
            h = reader.getHeight(reader.getMinIndex());
            w = reader.getWidth(reader.getMinIndex());
            ImageReadParam param = reader.getDefaultReadParam();
            Object inOb = null;
            try {
                for(int e1 = 0; e1 < h; ++e1) {
                    sourceRegion = new Rectangle(0, e1, w, 1);
                    param.setSourceRegion(sourceRegion);
                    image = reader.read(0, param);


                    inOb = (Object) Arrays.asList(((DataBufferUShort)image.getRaster().getDataBuffer()).getData());

                    ByteArrayOutputStream bos = new ByteArrayOutputStream();

                    ObjectOutput out = null;
                        try {
                            out = new ObjectOutputStream(bos);
                            out.writeObject(inOb);///записывает любой объект

                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    byte[] array_value_satelite = bos.toByteArray();

//                    short[] array_value_satelite = ((DataBufferUShort)image.getRaster().getDataBuffer()).getData();
                    byte[] array_configuration = new byte[]{(byte)0, (byte)0, (byte)0, 1};
                    byte[] sum_array = ArrayUtils.addAll(array_configuration, array_value_satelite);
                    image = null;
                    sourceRegion = null;

                    writer.append(new IntWritable(e1 + 1), new BytesWritable(sum_array));

                }
            } catch (IOException var34) {
                var34.printStackTrace();
            } finally {
                IOUtils.closeStream(writer);
//                System.out.println("Начало созданя файла последовательности: " + timeFormat.format(StartPro));
//                System.out.println("Конец созданя файла последовательности: " + timeFormat.format(new Date(System.currentTimeMillis())));
            }


//            sourceRegion = new Rectangle(0, 0, w, 1);
//
//            param.setSourceRegion(sourceRegion); // Set region
//
//            reader.read(0, param).getData();
//
// sourceRegion = new Rectangle(0, 0, w, h);
// param.setSourceRegion(sourceRegion); // Set region
//
// image = reader.read(0, param); // Will read only the region specified
//
// param = null;
// sourceRegion = null;
// reader = null;
//
// r = (WritableRaster) image.getData();
// r.getPixels(1, 1, w - 1, h - 1, ResultArray);
        }


//        byte[] formattedImageBytes; //*
//        formattedImageBytes = fimb.toByteArray();
//
//
//        ByteBuffer bb1 = ByteBuffer.wrap(formattedImageBytes);
//        ByteBuffer bb2 = ByteBuffer.wrap(formattedImageBytes);
//        bb1.order( ByteOrder.BIG_ENDIAN);
//        bb2.order( ByteOrder.LITTLE_ENDIAN);
//        ByteArraySeekableStream stream = null;//*
//
//        InputStream inputStream1 = new ByteArrayInputStream(bb1.array());
//        InputStream inputStream2 = new ByteArrayInputStream(bb2.array());
//
//        FileChannel fileChannel = (FileChannel) Channels.newChannel(inputStream1);
//        FileChannelImageInputStream fileChannelImageInputStream =
//                new FileChannelImageInputStream(fileChannel);



//        try {
//            stream = new ByteArraySeekableStream(formattedImageBytes, 0, formattedImageBytes.length);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        String[] names = ImageCodec.getDecoderNames(stream);
//        ImageDecoder dec = ImageCodec.createImageDecoder(names[0], stream, null);
//        RenderedImage im = dec.decodeAsRenderedImage();
//
//        Rectangle rectangle =  new Rectangle(0, 0, im.getWidth(), 1);
//
//        Raster raster = im.getData(rectangle);
    }
    }


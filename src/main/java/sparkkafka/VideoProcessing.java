package sparkkafka;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.opencv.core.Core;
import org.opencv.core.CvType;
import org.opencv.core.Mat;
import org.opencv.core.MatOfPoint;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
public class VideoProcessing implements Serializable{
    private static final Logger logger = Logger.getLogger(VideoProcessing.class);
    //load native lib
    static {
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
    }

    public static VideoEventData takeFrame(String camId, Iterator<VideoEventData> frames, String outputDir, VideoEventData previousProcessedEventData) throws Exception {
        VideoEventData currentProcessedEventData = new VideoEventData();
        Mat frame = null;
        Mat copyFrame = null;
        Mat grayFrame = null;
        Mat firstFrame = null;
        Mat deltaFrame = new Mat();
        Mat thresholdFrame = new Mat();
        ArrayList<Rect> rectArray = new ArrayList<Rect>();

        //previous processed frame
        if (previousProcessedEventData != null) {
            logger.warn("cameraId=" + camId + " previous processed timestamp=" + previousProcessedEventData.getTimestamp());
            Mat preFrame = getMat(previousProcessedEventData);
            firstFrame = preFrame;
        }

        //sort by timestamp
        ArrayList<VideoEventData> sortedList = new ArrayList<VideoEventData>();
        while(frames.hasNext()){
            sortedList.add(frames.next());
        }

        sortedList.sort(Comparator.comparing(VideoEventData::getTimestamp));  //need to investigate the type of this var
        logger.warn("cameraId="+camId+" total frames="+sortedList.size());

        //iterate and detect motion
        for (VideoEventData eventData : sortedList) {
            frame = getMat(eventData);

            logger.warn("Processing the event of cameraId=" + eventData.getCameraId() + ", timestamp="+ eventData.getTimestamp());
            if (firstFrame != null) {
                //save image file
                saveImage(frame, eventData, outputDir);
            }
            firstFrame = frame;
            currentProcessedEventData = eventData;
        }
        return currentProcessedEventData;
    }

    private static Mat getMat(VideoEventData ed) throws Exception{
        Mat mat = new Mat(ed.getRows(), ed.getCols(), ed.getType());
        mat.put(0, 0, Base64.getDecoder().decode(ed.getData()));
        return mat;
    }

    //Save image file
    private static void saveImage(Mat mat,VideoEventData ed,String outputDir){
        String timestamp = ed.getTimestamp().replaceAll("\\s+", "-").replace(".", "-").replace(":", "-");
        String imagePath = outputDir+ed.getCameraId()+"-T-"+timestamp+".png";
        logger.warn("Saving images to "+imagePath);
        boolean result = Imgcodecs.imwrite(imagePath, mat);
        if(!result){
            logger.error("Couldn't save images to path "+outputDir+".Please check if this path exists. This is configured in processed.output.dir key of property file.");
        }
    }
}

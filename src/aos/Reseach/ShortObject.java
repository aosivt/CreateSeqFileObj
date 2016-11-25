package aos.Reseach;

/**
 * Created by iskander on 25.11.16.
 */
public class ShortObject {
    private short[] data_from_tiff;

    public ShortObject(short[] data_from_tiff) {
        this.data_from_tiff = data_from_tiff;
    }

    public short[] getData_from_tiff() {
        return data_from_tiff;
    }

    public void setData_from_tiff(short[] data_from_tiff) {
        this.data_from_tiff = data_from_tiff;
    }
}

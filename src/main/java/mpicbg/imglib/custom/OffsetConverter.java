package mpicbg.imglib.custom;

public interface OffsetConverter
{
	public long[] roiOffsetToTileOffset( final int[] roiOffset );
	public double[] roiOffsetToTileOffset( final double[] roiOffset );

	public double[] tileOffsetToGlobalPosition( final long[] tileOffset );
}

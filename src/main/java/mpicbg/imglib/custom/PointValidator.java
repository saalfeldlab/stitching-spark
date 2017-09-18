package mpicbg.imglib.custom;

import net.imglib2.EuclideanSpace;

public interface PointValidator extends EuclideanSpace
{
	boolean testPoint( final double... coords );

	@Override
	int numDimensions();
}

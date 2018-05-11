package mpicbg.imglib.custom;

import net.imglib2.EuclideanSpace;

public interface OffsetValidator extends EuclideanSpace
{
	/**
	 * Tests whether a given offset between two tiles is valid.
	 * The offset is generally specified as {@code movingTilePos}&minus;{@code fixedTilePos}.
	 *
	 * @param offset
	 * @return
	 */
	boolean testOffset( double... offset );
}

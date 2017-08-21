package mpicbg.models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import net.imglib2.iterator.IntervalIterator;

public class SimilarityModelTest
{
	private static final int TILE_SIZE = 16;

	@Test
	public void test2DTranslation() throws NotEnoughDataPointsException
	{
		final List< PointMatch > matches = createMatches(
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 0, 0 },
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 10, 10 },
				2
			);
		final SimilarityModel2D model = new SimilarityModel2D();
		model.fit( matches );
		PointMatch.apply( matches, model );
		System.out.println( "2d translation: " + model + ", error=" + PointMatch.maxDistance( matches ) );
	}

	@Test
	public void test3DTranslation() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final List< PointMatch > matches = createMatches(
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 0, 0 },
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 10, 10 },
				3
			);
		final SimilarityModel3D model = new SimilarityModel3D();
		model.fit( matches );
		PointMatch.apply( matches, model );
		System.out.println( "3d translation: " + model + ", error=" + PointMatch.maxDistance( matches ) );
	}


	@Test
	public void test2DTranslationShrinking() throws NotEnoughDataPointsException
	{
		final List< PointMatch > matches = createMatches(
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 0, 0 },
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 1, -1 },
				2
			);
		final SimilarityModel2D model = new SimilarityModel2D();
		model.fit( matches );
		PointMatch.apply( matches, model );
		System.out.println( "2d translation+shrinking: " + model + ", error=" + PointMatch.maxDistance( matches ) );
	}

	@Test
	public void test3DTranslationShrinking() throws NotEnoughDataPointsException, IllDefinedDataPointsException
	{
		final List< PointMatch > matches = createMatches(
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 0, 0 },
				new int[] { TILE_SIZE / 4, 3 * TILE_SIZE / 4 }, new int[] { 1, -1 },
				3
			);
		final SimilarityModel3D model = new SimilarityModel3D();
		model.fit( matches );
		PointMatch.apply( matches, model );
		System.out.println( "3d translation+shrinking: " + model + ", error=" + PointMatch.maxDistance( matches ) );
	}


	private List< PointMatch > createMatches(
			final int[] pt1coords, final int[] pt1translation,
			final int[] pt2coords, final int[] pt2translation,
			final int dim )
	{
		final List< PointMatch > matches = new ArrayList<>();
		final int[] intervalDims = new int[ dim ];
		Arrays.fill( intervalDims, Math.max( Math.max( pt1coords.length, pt1translation.length ), Math.max( pt2coords.length, pt2translation.length ) ) );
		final IntervalIterator iter = new IntervalIterator( intervalDims );
		final int[] pos = new int[ iter.numDimensions() ];
		while ( iter.hasNext() )
		{
			iter.fwd();
			iter.localize( pos );
			final double[] pt1 = new double[ pos.length ], pt2 = new double[ pos.length ];
			for ( int d = 0; d < pos.length; ++d )
			{
				pt1[ d ] = pt1coords[ pos[ d ] ] + pt1translation[ pos[ d ] ];
				pt2[ d ] = pt2coords[ pos[ d ] ] + pt2translation[ pos[ d ] ];
			}
			matches.add( new PointMatch( new Point( pt1 ), new Point( pt2 ) ) );
		}
		return matches;
	}
}

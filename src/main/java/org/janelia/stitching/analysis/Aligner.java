package org.janelia.stitching.analysis;

import java.util.Arrays;
import java.util.Random;

import org.janelia.stitching.Boundaries;
import org.janelia.stitching.TileInfo;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.TileOperations;
import org.janelia.stitching.Utils;

import ij.IJ;
import ij.gui.Roi;
import mpicbg.stitching.ComparePair;
import mpicbg.stitching.ImageCollectionElement;
import mpicbg.stitching.ImagePlusTimePoint;
import mpicbg.stitching.PairWiseStitchingImgLib;
import mpicbg.stitching.PairWiseStitchingResult;
import mpicbg.stitching.StitchingParameters;

public class Aligner
{
	public static void main( final String[] args ) throws Exception
	{
		final int numIterations = 100;

		final TileInfo[] tiles1 = TileInfoJSONProvider.loadTilesConfiguration( args[ 0 ] );
		final TileInfo[] tiles2 = TileInfoJSONProvider.loadTilesConfiguration( args[ 1 ] );

		final Random rnd = new Random();
		final StitchingParameters params = new StitchingParameters();
		params.dimensionality = tiles1[ 0 ].numDimensions();
		params.channel1 = 0;
		params.channel2 = 0;
		params.timeSelect = 0;
		params.checkPeaks = 250;
		params.computeOverlap = true;
		params.subpixelAccuracy = false;
		params.virtual = true;
		params.absoluteThreshold = 7;
		params.relativeThreshold = 5;

		final double[] offset = new double[ tiles1[ 0 ].numDimensions() ];
		double highestCrossCorrelation = -Double.MAX_VALUE;

		for ( int iter = 0; iter < numIterations; iter++ )
		{
			final TileInfo t1 = tiles1[ rnd.nextInt( tiles1.length ) ];
			System.out.println( "Chosen tile " + t1.getIndex() + ", searching for the second tile..." );
			for ( final TileInfo t2 : tiles2 )
			{
				if ( TileOperations.getOverlappingRegionGlobal( t1, t2 ) == null )
					continue;

				System.out.println( "Stitching tiles " + t1.getIndex() + " and " + t2.getIndex() + "..." );

				final ImageCollectionElement el1 = Utils.createElement( t1 );
				final ImageCollectionElement el2 = Utils.createElement( t2 );

				final int timepoint = 1;
				final ComparePair pair = new ComparePair(
						new ImagePlusTimePoint( IJ.openImage( t1.getFile() ), el1.getIndex(), timepoint, el1.getModel(), el1 ),
						new ImagePlusTimePoint( IJ.openImage( t2.getFile() ), el2.getIndex(), timepoint, el2.getModel(), el2 ) );

				Roi roi1 = null, roi2 = null;

				final Boundaries overlap1 = TileOperations.getOverlappingRegion( t1, t2 );
				final Boundaries overlap2 = TileOperations.getOverlappingRegion( t2, t1 );

				// mpicbg accepts only 2d rectangular ROIs
				roi1 = new Roi( overlap1.min( 0 ), overlap1.min( 1 ), overlap1.dimension( 0 ), overlap1.dimension( 1 ) );
				roi2 = new Roi( overlap2.min( 0 ), overlap2.min( 1 ), overlap2.dimension( 0 ), overlap2.dimension( 1 ) );

				final double[] initialOffset = new double[ t1.numDimensions() ];
				for ( int d = 0; d < initialOffset.length; d++ )
					initialOffset[ d ] = t2.getPosition( d ) - t1.getPosition( d );

				final PairWiseStitchingResult result = PairWiseStitchingImgLib.stitchPairwise(
						pair.getImagePlus1(), pair.getImagePlus2(), roi1, roi2, pair.getTimePoint1(), pair.getTimePoint2(), params, initialOffset, t1.getSize() );

				System.out.println( "Stitched tiles " + t1.getIndex() + " and " + t2.getIndex() + System.lineSeparator() +
						"   CrossCorr=" + result.getCrossCorrelation() + ", PhaseCorr=" + result.getPhaseCorrelation() + ", RelShift=" + Arrays.toString( result.getOffset() ) );

				pair.getImagePlus1().close();
				pair.getImagePlus2().close();

				System.out.println( "Cross correlation = " + result.getCrossCorrelation() );
				System.out.println( "Offset = " + Arrays.toString( result.getOffset() ) );

				if ( highestCrossCorrelation < result.getCrossCorrelation() )
				{
					System.out.println( "----- best so far _-----" );
					highestCrossCorrelation = result.getCrossCorrelation();
					for ( int d = 0; d < offset.length; d++ )
						offset[ d ] = -( t2.getPosition( d ) - t1.getPosition( d ) - result.getOffset( d ) );
				}

				break;
			}
		}

		System.out.println();
		System.out.println( "Okay, the result is: ");
		System.out.println( "Cross correlation = " + highestCrossCorrelation );
		System.out.println( "Offset = " + Arrays.toString( offset ) );
	}
}

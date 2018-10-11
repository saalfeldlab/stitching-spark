package org.janelia.stitching;

import org.janelia.util.ImageImporter;

import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.imageplus.ImagePlusImgs;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.util.Util;

public class PairwiseStitchingTest
{
	public static < T extends RealType< T > & NativeType< T > > void main( final String[] args ) throws Exception
	{
//		final RandomAccessibleInterval< T > img1 = ImagePlusImgs.from( ImageImporter.openImage( "/nrs/saalfeld/igor/161211_YFP2_comparison2/new-config/new-affine-stitching/flatfield_ushort_tile-882_subtile-7057.tif" ) );
//		final RandomAccessibleInterval< T > img2 = ImagePlusImgs.from( ImageImporter.openImage( "/nrs/saalfeld/igor/161211_YFP2_comparison2/new-config/new-affine-stitching/flatfield_ushort_tile-883_subtile-7067.tif" ) );

		final RandomAccessibleInterval< T > img1 = ImagePlusImgs.from( ImageImporter.openImage( "/nrs/saalfeld/igor/161211_YFP2_comparison2/new-config/new-affine-stitching/new-phase-correlation-code-ushort/bounding-box_smallest-containing-interval/test-two-tiles/unconstrained/tile-882_subtile-1.tif" ) );
		final RandomAccessibleInterval< T > img2 = ImagePlusImgs.from( ImageImporter.openImage( "/nrs/saalfeld/igor/161211_YFP2_comparison2/new-config/new-affine-stitching/new-phase-correlation-code-ushort/bounding-box_smallest-containing-interval/test-two-tiles/unconstrained/tile-883_subtile-11.tif" ) );

		final SerializableStitchingParameters params = new SerializableStitchingParameters();
		params.checkPeaks = 10;

		@SuppressWarnings( "unchecked" )
		final SerializablePairWiseStitchingResult result = StitchSubTilePair.stitchPairwise( new RandomAccessibleInterval[] { img1, img2 }, params, null );

		final double expectedCrossCorrelation = 0.98;
		if ( !Util.isApproxEqual( result.getCrossCorrelation(), expectedCrossCorrelation, 0.01 ) )
			throw new Exception( String.format( "Expected high cross correlation (>%.2f), got %.2f", expectedCrossCorrelation, result.getCrossCorrelation() ) );

		final double[] expectedOffset = new double[] { -3.651121491626526, -313.2724166510346, -4.057084141577434 };
		for ( int d = 0; d < expectedOffset.length; ++d )
			if ( !Util.isApproxEqual( result.getOffset( d ), expectedOffset[ d ], 2 ) ) // allow to vary within subpixel precision
				throw new Exception( String.format(
						"expected offset [%.2f, %.2f, %.2f], got [%.2f, %.2f, %.2f]",
						expectedOffset[ 0 ], expectedOffset[ 1 ], expectedOffset[ 2 ],
						result.getOffset( 0 ), result.getOffset( 1 ), result.getOffset( 2 )
					) );

		System.out.println( "OK" );
	}
}

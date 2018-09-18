package org.janelia.stitching.analysis;

import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.janelia.dataaccess.DataProvider;
import org.janelia.dataaccess.DataProviderFactory;
import org.janelia.stitching.SerializablePairWiseStitchingResult;
import org.janelia.stitching.TileInfoJSONProvider;
import org.janelia.stitching.Utils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import net.imglib2.util.Pair;
import net.imglib2.util.Util;
import net.imglib2.util.ValuePair;

public class CompareEstimatedShiftsConstrainedVsUnconstrained
{
	private static class CompareEstimatedShiftsCmdArgs implements Serializable
	{
		private static final long serialVersionUID = 215043103837732209L;

		@Option(name = "-c", aliases = { "--constrainedPairwisePath" }, required = true,
				usage = "Path to the constrained pairwise configuration file.")
		public String constrainedPairwisePath;

		@Option(name = "-uc", aliases = { "--unconstrainedPairwisePath" }, required = true,
				usage = "Path to the unconstrained pairwise configuration file.")
		public String unconstrainedPairwisePath;

		public boolean parsedSuccessfully = false;

		public CompareEstimatedShiftsCmdArgs( final String... args ) throws IllegalArgumentException
		{
			final CmdLineParser parser = new CmdLineParser( this );
			try
			{
				parser.parseArgument( args );
				parsedSuccessfully = true;
			}
			catch ( final CmdLineException e )
			{
				System.err.println( e.getMessage() );
				parser.printUsage( System.err );
			}
		}
	}

	public static void main( final String[] args ) throws Exception
	{
		final CompareEstimatedShiftsCmdArgs parsedArgs = new CompareEstimatedShiftsCmdArgs( args );
		if ( !parsedArgs.parsedSuccessfully )
			System.exit( 1 );

		final DataProvider dataProvider = DataProviderFactory.createFSDataProvider();

		final List< SerializablePairWiseStitchingResult > constrainedPairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.constrainedPairwisePath ) ) );
		final List< SerializablePairWiseStitchingResult > unconstrainedPairwiseShifts = TileInfoJSONProvider.loadPairwiseShifts( dataProvider.getJsonReader( URI.create( parsedArgs.unconstrainedPairwisePath ) ) );
		System.out.println( String.format(
				"Num total pairs: constrained=%d, unconstrained=%d",
				constrainedPairwiseShifts.size(),
				unconstrainedPairwiseShifts.size()
			) );

		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > constrainedPairwiseShiftsMap = Utils.createSubTilePairwiseResultsMap( constrainedPairwiseShifts, false );
		final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > unconstrainedPairwiseShiftsMap = Utils.createSubTilePairwiseResultsMap( unconstrainedPairwiseShifts, false );
		System.out.println( String.format(
				"Num extra pairs: constrained=%d, unconstrained=%d",
				countExtraPairsInA( constrainedPairwiseShiftsMap, unconstrainedPairwiseShiftsMap ),
				countExtraPairsInA( unconstrainedPairwiseShiftsMap, constrainedPairwiseShiftsMap )
			) );


		final List< SerializablePairWiseStitchingResult > commonBestShifts = new ArrayList<>();
		final List< Pair< SerializablePairWiseStitchingResult, SerializablePairWiseStitchingResult > > mostDisagreeingShifts = new ArrayList<>();

		try ( final PrintWriter fileWriter = new PrintWriter( "constrained-vs-unconstrained.txt" ) )
		{
			fileWriter.println( "constrained_cr.corr unconstrained_cr.corr constrained_ph.corr unconstrained_ph.corr" );
			for ( final Entry< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > constrainedPairwiseShiftOuterEntry : constrainedPairwiseShiftsMap.entrySet() )
			{
				for ( final Entry< Integer, SerializablePairWiseStitchingResult > constrainedPairwiseShiftInnerEntry : constrainedPairwiseShiftOuterEntry.getValue().entrySet() )
				{
					final SerializablePairWiseStitchingResult constrainedPairwiseShift = constrainedPairwiseShiftInnerEntry.getValue();
					final SerializablePairWiseStitchingResult unconstrainedPairwiseShift = unconstrainedPairwiseShiftsMap.get( constrainedPairwiseShiftOuterEntry.getKey() ).get( constrainedPairwiseShiftInnerEntry.getKey() );
					fileWriter.println( String.format(
							"%.5f %.5f %.5f %.5f",
							constrainedPairwiseShift.getCrossCorrelation(), unconstrainedPairwiseShift.getCrossCorrelation(),
							constrainedPairwiseShift.getPhaseCorrelation(), unconstrainedPairwiseShift.getPhaseCorrelation()
						) );

					if ( constrainedPairwiseShift.getCrossCorrelation() > 0.98 && Util.isApproxEqual( constrainedPairwiseShift.getCrossCorrelation(), unconstrainedPairwiseShift.getCrossCorrelation(), 1e-5 ) )
						commonBestShifts.add( constrainedPairwiseShift );

					if ( constrainedPairwiseShift.getCrossCorrelation() > 0.98 && unconstrainedPairwiseShift.getCrossCorrelation() < 0.02 )
						mostDisagreeingShifts.add( new ValuePair<>( constrainedPairwiseShift, unconstrainedPairwiseShift ) );
				}
			}
		}

		System.out.println();
		System.out.println( "Common best shifts:" );
		for ( final SerializablePairWiseStitchingResult shift : commonBestShifts )
			System.out.println( String.format( "%s, cr.corr=%.5f", shift.getSubTilePair(), shift.getCrossCorrelation() ) );

		System.out.println();
		System.out.println( "Most disagreeing shifts:" );
		for ( final Pair< SerializablePairWiseStitchingResult, SerializablePairWiseStitchingResult > shiftPair : mostDisagreeingShifts ) {
			if ( shiftPair.getA().getSubTilePair().getA().getIndex().intValue() != shiftPair.getB().getSubTilePair().getA().getIndex().intValue() || shiftPair.getA().getSubTilePair().getB().getIndex().intValue() != shiftPair.getB().getSubTilePair().getB().getIndex().intValue() )
				throw new RuntimeException( "different subtile pairs" );
			System.out.println( String.format( "%s, constrained cr.corr=%.5f, unconstrained cr.corr=%.5f,  constrained shift (probably correct)=%s, unconstrained shift=%s",
					shiftPair.getA().getSubTilePair(),
					shiftPair.getA().getCrossCorrelation(),
					shiftPair.getB().getCrossCorrelation(),
					Arrays.toString( shiftPair.getA().getOffset() ),
					Arrays.toString( shiftPair.getB().getOffset() )
				) );
		}

		System.out.println( System.lineSeparator() + "Done" );
	}

	private static int countExtraPairsInA(
			final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > a,
			final Map< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > b )
	{
		int numExtraPairs = 0;
		for ( final Entry< Integer, ? extends Map< Integer, SerializablePairWiseStitchingResult > > outerEntry : a.entrySet() )
			for ( final Entry< Integer, SerializablePairWiseStitchingResult > innerEntry : outerEntry.getValue().entrySet() )
				if ( !b.containsKey( outerEntry.getKey() ) || !b.get( outerEntry.getKey() ).containsKey( innerEntry.getKey() ) )
					++numExtraPairs;
		return numExtraPairs;
	}
}

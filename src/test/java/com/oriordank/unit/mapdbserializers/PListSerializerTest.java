package com.oriordank.unit.mapdbserializers;

import com.oriordank.mapdbserializers.PListEntrySerializer;
import org.apache.activemq.store.PListEntry;
import org.apache.activemq.util.ByteSequence;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class PListSerializerTest {

    @Test
    public void testSerializeDeserialize() throws IOException {
        DataOutput2 dataOutput = new DataOutput2();
        PListEntrySerializer serializer = new PListEntrySerializer();
        byte[] data = {1, 2, 3, 4, 5, 6};
        ByteSequence bs = new ByteSequence(data, 1, 4);
        PListEntry entry = new PListEntry("Test", bs, 1l);
        serializer.serialize(dataOutput, entry);
        DataInput2 dataInput = new DataInput2(dataOutput.copyBytes());
        PListEntry deserializedEntry = serializer.deserialize(dataInput, dataInput.available());
        assertThat(deserializedEntry.getId(), is(entry.getId()));
        assertThat(deserializedEntry.getLocator(), is(entry.getLocator()));
        assertThat(deserializedEntry.getByteSequence(), byteSequenceEquals(entry.getByteSequence()));
    }

    private Matcher<? super ByteSequence> byteSequenceEquals(final ByteSequence expectedBs) {
        return new ArrayEqualsUsingLengthAndOffset(expectedBs);
    }

    private class ArrayEqualsUsingLengthAndOffset extends TypeSafeMatcher<ByteSequence> {

        private final ByteSequence expectedBs;

        public ArrayEqualsUsingLengthAndOffset(ByteSequence expectedBs) {
            this.expectedBs = expectedBs;
        }

        @Override
        protected boolean matchesSafely(ByteSequence actualBs) {
            if (actualBs.getLength() != expectedBs.getLength()) {
                return false;
            }
            byte[] actualData = actualBs.data;
            byte[] expectedData = expectedBs.data;
            for (int i=0; i<expectedBs.length; i++) {
                if (actualData[actualBs.offset+i] != expectedData[expectedBs.getOffset() + i]) {
                    return false;
                }

            }
            return true;
        }

        @Override
        public void describeTo(Description description) {

        }
    }

}
